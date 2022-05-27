/*
 * Copyright 2021 Abhinav Chawade
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.github.drnushooz.kafka.connect.sqs.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.drnushooz.kafka.connect.About;
import com.github.drnushooz.kafka.connect.sqs.ConnectorConfig;
import com.github.drnushooz.kafka.connect.sqs.ConnectorConfigKeys;
import com.github.drnushooz.kafka.connect.sqs.SqsClientFactory;
import io.apicurio.registry.utils.converter.avro.AvroData;
import io.apicurio.registry.utils.converter.avro.AvroDataConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.talend.daikon.avro.converter.JsonGenericRecordConverter;
import org.talend.daikon.avro.inferrer.JsonSchemaInferrer;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

@Slf4j
public class SqsSourceTask extends SourceTask {
    private SqsClient client;
    private ConnectorConfig connectorConfig;

    private String queueUrl;
    private String topic;
    private int maxMessages;
    private int waitTimeSeconds;

    private JsonSchemaInferrer schemaInferrer;
    private AvroData avroData;

    @Getter
    HashMap<String, Object> combinedProperties;

    @Override
    public String version() {
        // Populated by the templating plugin
        return About.CURRENT_VERSION;
    }

    @Override
    public void start(@NonNull Map<String, String> props) {
        connectorConfig = new ConnectorConfig(props);
        combinedProperties = new HashMap<>(connectorConfig.originalsStrings());
        combinedProperties.putAll(props);
        combinedProperties
            .put(ConnectorConfigKeys.SQS_CREDENTIALS_USE_DEFAULT_PROVIDER.getValue(),
                connectorConfig.isCredentialsUseDefaultProvider());
        client = SqsClientFactory.getClient(combinedProperties);
        queueUrl = connectorConfig.getQueueUrl();
        maxMessages = connectorConfig.getMaxMessages();
        waitTimeSeconds = connectorConfig.getWaitTimeSeconds();
        topic = connectorConfig.getTopic();
        schemaInferrer = new JsonSchemaInferrer(
            new ObjectMapper().configure(DeserializationFeature.USE_LONG_FOR_INTS,
                connectorConfig.isUseLongForInts()));
        avroData = new AvroData(new AvroDataConfig(props));
        log.info("Source task started for queue URL: {} and topic: {}", queueUrl, topic);
        if (connectorConfig.isUseLongForInts()) {
            log.info("Using long to represent int in incoming JSON");
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (!isValidState()) {
            throw new IllegalStateException("Task is not properly initialized");
        }

        CompletableFuture<List<Message>> messagesFut = CompletableFuture.supplyAsync(() -> {
            ReceiveMessageRequest request =
                ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(maxMessages)
                    .waitTimeSeconds(waitTimeSeconds)
                    .build();
            ReceiveMessageResponse response = client.receiveMessage(request);
            List<Message> messages = response.messages();

            log.debug(
                "Polling queue URL: {} max messages: {} max wait: {} size: {}",
                queueUrl,
                maxMessages,
                waitTimeSeconds,
                messages.size());

            return messages;
        });

        CompletableFuture<List<SourceRecord>> recordsFut =
            messagesFut.thenCompose((List<Message> messages) -> {
                List<SourceRecord> records = messages.parallelStream().map(message -> {
                    Map<String, String> sourcePartition =
                        Collections.singletonMap(ConnectorConfigKeys.SQS_QUEUE_URL.getValue(),
                            queueUrl);
                    Map<String, String> sourceOffset = new HashMap<>();

                    // Message ID and receipt-handle are used to delete a message once a is committed
                    sourceOffset.put(ConnectorConfigKeys.SQS_MESSAGE_ID.getValue(),
                        message.messageId());
                    sourceOffset.put(
                        ConnectorConfigKeys.SQS_MESSAGE_RECEIPT_HANDLE.getValue(),
                        message.receiptHandle());

                    if (log.isTraceEnabled()) {
                        log.trace("Poll source-partition: {}", sourcePartition);
                        log.trace("Poll source-offset: {}", sourceOffset);
                    }

                    // Generate the output record
                    String messageKey = message.messageId();
                    String messageValue = message.body();
                    Schema messageValueAvroSchema = schemaInferrer.inferSchema(messageValue);
                    JsonGenericRecordConverter recordConverter =
                        new JsonGenericRecordConverter(messageValueAvroSchema);
                    GenericRecord outputRecord = recordConverter.convertToAvro(messageValue);
                    SchemaAndValue connectSchemaAndData =
                        avroData.toConnectData(messageValueAvroSchema, outputRecord);

                    return new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        topic,
                        org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                        messageKey,
                        connectSchemaAndData.schema(),
                        connectSchemaAndData.value());
                }).collect(Collectors.toList());

                return CompletableFuture.completedFuture(records);
            });

        return recordsFut.join();
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata)
        throws InterruptedException {
        final String receiptHandle =
            record.sourceOffset().get(ConnectorConfigKeys.SQS_MESSAGE_RECEIPT_HANDLE.getValue())
                .toString();
        DeleteMessageRequest request =
            DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receiptHandle).build();
        client.deleteMessage(request);
        log.debug("Deleted message with handle: {}", receiptHandle);
        super.commitRecord(record, metadata);
    }

    @Override
    public void stop() {
        log.info("Source task stopped");
    }

    private boolean isValidState() {
        return connectorConfig != null && client != null;
    }
}
