/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gcote.eventbus.broker;

import org.apache.rocketmq.broker.BrokerPathConfigHelper;

import java.io.File;

public class EventBrokerPathConfigHelper extends BrokerPathConfigHelper {
    private static String topicConfigPath = System.getProperty("user.home") + File.separator + "topicConfig";
    private static String brokerConfigPath = System.getProperty("user.home") + File.separator + "brokerConfig";

    public static String getTopicConfigPath() {
        return topicConfigPath;
    }

    public static void setTopicConfigPath(String topicConfigPath) {
        EventBrokerPathConfigHelper.topicConfigPath = topicConfigPath;
    }

    public static String getBrokerConfigPath() {
        return brokerConfigPath;
    }

    public static void setBrokerConfigPath(String brokerConfigPath) {
        EventBrokerPathConfigHelper.brokerConfigPath = brokerConfigPath;
    }
}
