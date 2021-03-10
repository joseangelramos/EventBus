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
package com.gcote.eventbus.broker.topic;

import com.gcote.eventbus.common.protocol.EventBusTopicConfig;
import com.gcote.eventbus.common.protocol.body.EventBusTopicConfigSerializeWrapper;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class EventTopicConfigManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final ConcurrentHashMap<String, EventBusTopicConfig> extTopicConfigTable =
        new ConcurrentHashMap<String, EventBusTopicConfig>(1024);
    private final DataVersion dataVersion = new DataVersion();
    private final Set<String> systemTopicList = new HashSet<String>();
    private transient BrokerController brokerController;

    public EventTopicConfigManager() {
    }

    public EventTopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        {
            // MixAll.SELF_TEST_TOPIC
            String topic = MixAll.SELF_TEST_TOPIC;
            EventBusTopicConfig eventBusTopicConfig = new EventBusTopicConfig(topic);
            this.systemTopicList.add(topic);
            this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
        }

        {
            // MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC
            if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
                String topic = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;
                EventBusTopicConfig eventBusTopicConfig = new EventBusTopicConfig(topic);
                this.systemTopicList.add(topic);
                this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
            }
        }
        {
            // MixAll.BENCHMARK_TOPIC
            String topic = MixAll.BENCHMARK_TOPIC;
            EventBusTopicConfig eventBusTopicConfig = new EventBusTopicConfig(topic);
            this.systemTopicList.add(topic);
            this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
        }
        {

            String topic = this.brokerController.getBrokerConfig().getBrokerClusterName();
            EventBusTopicConfig eventBusTopicConfig = new EventBusTopicConfig(topic);
            this.systemTopicList.add(topic);
            this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
        }
        {

            String topic = this.brokerController.getBrokerConfig().getBrokerName();
            EventBusTopicConfig eventBusTopicConfig = new EventBusTopicConfig(topic);
            this.systemTopicList.add(topic);
            this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
        }
        {
            // MixAll.OFFSET_MOVED_EVENT
            String topic = MixAll.OFFSET_MOVED_EVENT;
            EventBusTopicConfig eventBusTopicConfig = new EventBusTopicConfig(topic);
            this.systemTopicList.add(topic);
            this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
        }
        {
            String rr_reply_topic = this.brokerController.getBrokerConfig().getBrokerClusterName() + "-rr-reply-topic";
            EventBusTopicConfig eventBusTopicConfig = new EventBusTopicConfig(rr_reply_topic);
            this.systemTopicList.add(rr_reply_topic);
            this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
        }
    }

    public boolean isSystemTopic(final String topic) {
        return this.systemTopicList.contains(topic);
    }

    public Set<String> getSystemTopic() {
        return this.systemTopicList;
    }

    public boolean isTopicCanSendMessage(final String topic) {
        return !topic.equals(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC);
    }

    public void updateTopicConfig(final EventBusTopicConfig eventBusTopicConfig) {
        EventBusTopicConfig old = this.extTopicConfigTable.put(eventBusTopicConfig.getTopicName(), eventBusTopicConfig);
        if (old != null) {
            log.info("update ext topic config, old: " + old + " new: " + eventBusTopicConfig);
        } else {
            log.info("create new ext topic, " + eventBusTopicConfig);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    public EventBusTopicConfig selectExtTopicConfig(final String topic) {
        if (this.brokerController.getTopicConfigManager().selectTopicConfig(topic) == null) {
            this.extTopicConfigTable.remove(topic);
            this.persist();
            return null;
        }

        //This scenario may exists in auto-create topics
        if (this.extTopicConfigTable.get(topic) == null) {
            extTopicConfigTable.put(topic, new EventBusTopicConfig(topic));
            this.persist();
        }

        return this.extTopicConfigTable.get(topic);
    }

    public void deleteExtTopicConfig(final String topic) {
        EventBusTopicConfig old = this.extTopicConfigTable.remove(topic);
        if (old != null) {
            log.info("delete topic config OK, topic: " + old);
            this.dataVersion.nextVersion();
            this.persist();
        } else {
            log.warn("delete topic config failed, topic: " + topic + " not exist");
        }
    }

    public EventBusTopicConfigSerializeWrapper buildExtTopicConfigSerializeWrapper() {
        EventBusTopicConfigSerializeWrapper ExtTopicConfigSerializeWrapper = new EventBusTopicConfigSerializeWrapper();
        ExtTopicConfigSerializeWrapper.setExtTopicConfigTable(this.extTopicConfigTable);
        ExtTopicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return ExtTopicConfigSerializeWrapper;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        String configPath = BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());

        return configPath.replaceAll("topics.json", "topicsExt.json");
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            EventBusTopicConfigSerializeWrapper extTopicConfigSerializeWrapper =
                    EventBusTopicConfigSerializeWrapper.fromJson(jsonString, EventBusTopicConfigSerializeWrapper.class);
            if (extTopicConfigSerializeWrapper != null) {
                this.extTopicConfigTable.putAll(extTopicConfigSerializeWrapper.getExtTopicConfigTable());
                this.dataVersion.assignNewOne(extTopicConfigSerializeWrapper.getDataVersion());
                this.printLoadDataWhenFirstBoot(extTopicConfigSerializeWrapper);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        //check consistency of TopicConfigManager and EventTopicConfigManager
        boolean isChanged = false;
        for (Entry<String, TopicConfig> entry : this.brokerController.getTopicConfigManager().getTopicConfigTable().entrySet()) {
            String topic = entry.getKey();
            if (this.extTopicConfigTable.get(topic) == null) {
                this.extTopicConfigTable.put(topic, new EventBusTopicConfig(topic));
                isChanged = true;
            }
        }
        if (isChanged) {
            log.info("topicConfigManager is not consistent with extTopicConfigManager, auto fix it when encode");
            this.persist();
        }

        EventBusTopicConfigSerializeWrapper ExtTopicConfigSerializeWrapper = new EventBusTopicConfigSerializeWrapper();
        ExtTopicConfigSerializeWrapper.setExtTopicConfigTable(this.extTopicConfigTable);
        ExtTopicConfigSerializeWrapper.setDataVersion(this.dataVersion);
        return ExtTopicConfigSerializeWrapper.toJson(prettyFormat);
    }

    private void printLoadDataWhenFirstBoot(final EventBusTopicConfigSerializeWrapper tcs) {
        Iterator<Entry<String, EventBusTopicConfig>> it = tcs.getExtTopicConfigTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, EventBusTopicConfig> next = it.next();
            log.info("load exist local topic, {}", next.getValue().toString());
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void clear() {
        this.extTopicConfigTable.clear();
    }

    public void addAll(ConcurrentHashMap<String, EventBusTopicConfig> table) {
        this.extTopicConfigTable.putAll(table);
    }
}
