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

import lombok.Getter;

public enum ConnectorConfigKeys {
    SQS_MAX_MESSAGES("sqs.max.messages"),
    SQS_QUEUE_URL("sqs.queue.url"),
    SQS_WAIT_TIME_SECONDS("sqs.wait.time.seconds"),
    TOPIC("topic"),

    SQS_CREDENTIALS_USE_DEFAULT_PROVIDER("sqs.credentials.use.default.provider"),

    // Message ID and receipt-handle are used to delete a message once a is committed
    SQS_MESSAGE_ID("sqs.message.id"),
    SQS_MESSAGE_RECEIPT_HANDLE("sqs.message.receipt-handle"),

    SCHEMA_USE_LONG_FOR_INTS("schema.use.long.for.ints");

    @Getter
    private final String value;

    ConnectorConfigKeys(String value) {
        this.value = value;
    }
}
