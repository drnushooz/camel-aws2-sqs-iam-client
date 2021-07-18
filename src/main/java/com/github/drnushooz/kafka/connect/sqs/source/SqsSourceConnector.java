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

import com.github.drnushooz.kafka.connect.About;
import com.github.drnushooz.kafka.connect.sqs.ConnectorConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqsSourceConnector extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());

    private Map<String, String> configuration;

    @Override
    public ConfigDef config() {
        return ConnectorConfig.getConfig();
    }

    @Override
    public String version() {
        // Populated by the templating plugin
        return About.CURRENT_VERSION;
    }

    @Override
    public void start(Map<String, String> configuration) {
        this.configuration = configuration;
        logger.info("Source connector started with properties: {}", configuration);
    }

    @Override
    public void stop() {
        logger.info("Source connector stopped");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SqsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        Map<String, String> taskProps = Map.copyOf(configuration);
        IntStream tasksIds = IntStream.range(0, maxTasks);
        tasksIds.parallel().forEach(i -> taskConfigs.add(taskProps));
        return taskConfigs;
    }
}
