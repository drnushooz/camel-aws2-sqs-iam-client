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

package com.github.drnushooz.kafka.connect.sqs.sink;

import com.github.drnushooz.kafka.connect.About;
import com.github.drnushooz.kafka.connect.sqs.ConnectorConfig;
import com.github.drnushooz.kafka.connect.sqs.ConnectorConfigKeys;
import com.github.drnushooz.kafka.connect.sqs.SqsClientFactory;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SqsSinkTask extends SinkTask {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());

    private SqsClient client;
    private ConnectorConfig connectorConfig;

    private String queueUrl;

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
            .put(ConnectorConfigKeys.SQS_CREDENTIALS_USE_DEFAULT_PROVIDER.getValue(), connectorConfig.isCredentialsUseDefaultProvider());
        client = SqsClientFactory.getClient(combinedProperties);
        queueUrl = connectorConfig.getQueueUrl();
        logger.info(
            "Sink task started for queue URL: {} and topic: {}", queueUrl, connectorConfig.getTopic());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        if (!isValidState()) {
            throw new IllegalStateException("Task is not properly initialized");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Put record count: {}", records.size());
        }

        records.forEach(
            record -> {
                final String messageId =
                    MessageFormat.format(
                        "{0}-{1}-{2}",
                        record.topic(), record.kafkaPartition().longValue(), record.kafkaOffset());
                final String recordKey =
                    Optional.ofNullable(record.key()).map(Object::toString).orElse(null);
                final String recordTopic =
                    Optional.ofNullable(recordKey).filter(String::isEmpty).orElse(record.topic());
                final String recordValue =
                    Optional.ofNullable(record.value()).map(Object::toString).orElse("");

                if (recordValue.isEmpty()) {
                    logger.warn("Skipping empty message with record key: {}", recordKey);
                } else {
                    try {
                        final SendMessageRequest request =
                            SendMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .messageBody(recordValue)
                                .messageGroupId(recordTopic)
                                .messageDeduplicationId(messageId)
                                .build();
                        SendMessageResponse response = client.sendMessage(request);
                        logger.debug(
                            "Sent message ID: {} queue URL: {} SQS group ID: {} SQS message ID: {}",
                            recordTopic,
                            messageId,
                            queueUrl,
                            response.messageId());
                    } catch (final RuntimeException e) {
                        logger.error(
                            "An Exception occurred while sending message ID: {} to target URL: {}",
                            messageId,
                            queueUrl,
                            e);
                    }
                }
            });
    }

    @Override
    public void stop() {
        logger.info("Sink task stopped");
    }

    private boolean isValidState() {
        return connectorConfig != null && client != null;
    }
}
