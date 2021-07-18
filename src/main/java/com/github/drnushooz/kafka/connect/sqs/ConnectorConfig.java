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

package com.github.drnushooz.kafka.connect.sqs;

import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ConnectorConfig extends AbstractConfig {

    @Getter
    private static final ConfigDef config =
        new ConfigDef()
            .define(
                ConnectorConfigKeys.SQS_MAX_MESSAGES.getValue(),
                ConfigDef.Type.INT,
                1,
                ConfigDef.Importance.LOW,
                "Maximum number of messages to read from SQS queue for each poll interval. Range is 0 - 10 with default of 1.")
            .define(
                ConnectorConfigKeys.SQS_QUEUE_URL.getValue(),
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "The ARN of the SQS queue to be read from.")
            .define(
                ConnectorConfigKeys.TOPIC.getValue(),
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Kafka topic to write to (source) or read from (sink).")
            .define(
                ConnectorConfigKeys.SQS_WAIT_TIME_SECONDS.getValue(),
                ConfigDef.Type.INT,
                1,
                ConfigDef.Importance.LOW,
                "Duration (in seconds) to wait for a message to arrive in the queue. Default is 1.")
            .define(
                ConnectorConfigKeys.SQS_CREDENTIALS_USE_DEFAULT_PROVIDER.getValue(),
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.HIGH,
                "A boolean indicating if default credentials provider should be used. Default is false.")
            .define(
                ConnectorConfigKeys.SCHEMA_USE_LONG_FOR_INTS.getValue(),
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.HIGH,
                "Treat integers in the input JSON as longs. Default is false.");

    @Getter
    private final int maxMessages;
    @Getter
    private final String queueUrl;
    @Getter
    private final String topic;
    @Getter
    private final int waitTimeSeconds;
    @Getter
    private final boolean credentialsUseDefaultProvider;
    @Getter
    private final boolean useLongForInts;

    public ConnectorConfig(Map<?, ?> originals) {
        super(getConfig(), originals);
        maxMessages = getInt(ConnectorConfigKeys.SQS_MAX_MESSAGES.getValue());
        queueUrl = getString(ConnectorConfigKeys.SQS_QUEUE_URL.getValue());
        topic = getString(ConnectorConfigKeys.TOPIC.getValue());
        waitTimeSeconds = getInt(ConnectorConfigKeys.SQS_WAIT_TIME_SECONDS.getValue());
        credentialsUseDefaultProvider =
            getBoolean(ConnectorConfigKeys.SQS_CREDENTIALS_USE_DEFAULT_PROVIDER.getValue());
        useLongForInts =
            getBoolean(ConnectorConfigKeys.SCHEMA_USE_LONG_FOR_INTS.getValue());
    }
}
