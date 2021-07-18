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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.drnushooz.kafka.connect.sqs.ConnectorConfig;
import com.github.drnushooz.kafka.connect.sqs.ConnectorConfigKeys;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class ConnectorConfigTest {
    @Test
    void testMissingImportantValue() {
        Map<String, String> original = new HashMap<>();
        original.put(ConnectorConfigKeys.SQS_QUEUE_URL.getValue(), "nonexistentqueue");
        assertThrows(ConfigException.class, () -> new ConnectorConfig(original));
    }
}