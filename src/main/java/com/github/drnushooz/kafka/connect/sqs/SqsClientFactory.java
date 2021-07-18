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
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SqsClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(SqsClientFactory.class);
    private static final ConcurrentHashMap<Map<String, ?>, SqsClient> sqsClientsWithConfig =
        new ConcurrentHashMap<>();

    public static SqsClient getClient(Map<String, ?> connectorConfig) {
        return sqsClientsWithConfig.computeIfAbsent(
            connectorConfig,
            config -> {
                AwsCredentialsProvider credentialsProvider;
                boolean useDefaultCredentialsProvider =
                    Boolean.parseBoolean(config.get(ConnectorConfigKeys.SQS_CREDENTIALS_USE_DEFAULT_PROVIDER.getValue()).toString());
                if (useDefaultCredentialsProvider) {
                    logger.info("Using default credentials provider");
                    credentialsProvider = DefaultCredentialsProvider.create();
                } else {
                    String roleArn = SdkSystemSetting.AWS_ROLE_ARN.getStringValueOrThrow();
                    String tokenFile = SdkSystemSetting.AWS_WEB_IDENTITY_TOKEN_FILE.getStringValueOrThrow();
                    logger.info("Using web identity credentials provider with role ARN: {} and token file: {}", roleArn, tokenFile);
                    credentialsProvider = WebIdentityTokenFileCredentialsProvider.create();
                }
                return SqsClient.builder().credentialsProvider(credentialsProvider).build();
            });
    }
}
