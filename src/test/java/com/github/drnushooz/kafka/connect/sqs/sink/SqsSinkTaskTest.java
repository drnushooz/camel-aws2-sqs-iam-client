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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.drnushooz.kafka.connect.sqs.ConnectorConfigKeys;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SqsSinkTaskTest {
    @Test
    void testPropertyOverriding() {
        Map<String, String> props = new HashMap<>();
        props.put(ConnectorConfigKeys.SQS_QUEUE_URL.getValue(), "queue");
        props.put(ConnectorConfigKeys.TOPIC.getValue(), "topic");
        props.put(ConnectorConfigKeys.SQS_CREDENTIALS_USE_DEFAULT_PROVIDER.getValue(), "true");

        SqsSinkTask task = new SqsSinkTask();
        task.start(props);
        Map<String, Object> retrievedProps = task.getCombinedProperties();
        assertEquals(retrievedProps.get(ConnectorConfigKeys.SQS_CREDENTIALS_USE_DEFAULT_PROVIDER.getValue()), true);
    }
}
