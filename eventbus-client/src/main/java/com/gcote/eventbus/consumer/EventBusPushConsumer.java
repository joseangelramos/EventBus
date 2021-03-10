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

package com.gcote.eventbus.consumer;

import com.gcote.eventbus.client.EventBusClientManager;
import com.gcote.eventbus.client.common.EventBusClientConfig;
import com.gcote.eventbus.client.impl.EventBusClientAPIImpl;
import com.gcote.eventbus.client.impl.factory.EventBusClientInstance;
import com.gcote.eventbus.client.impl.hook.EventBusClientHookFactory;
import com.gcote.eventbus.client.impl.rebalance.AllocateMessageQueueByIDC;
import com.gcote.eventbus.common.EventBusConstant;
import com.gcote.eventbus.common.EventBusVersion;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventBusPushConsumer {
    protected static final Logger LOG = LoggerFactory.getLogger(EventBusPushConsumer.class);

    private DefaultMQPushConsumer defaultMQPushConsumer;
    private EventBusClientInstance eventBusClientInstance;
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    private RPCHook rpcHook;
    private EventBusClientConfig eventBusClientConfig;
    private AtomicBoolean isStart = new AtomicBoolean(false);

    static {
        System.setProperty("rocketmq.client.log.loadconfig", "false");
    }

    public EventBusPushConsumer() {
        this(new EventBusClientConfig());
    }

    public EventBusPushConsumer(final EventBusClientConfig eventBusClientConfig) {
        this.eventBusClientConfig = eventBusClientConfig;
        RPCHook rpcHookForAuth = EventBusClientHookFactory.createRPCHook(eventBusClientConfig.getRpcHook());
        this.rpcHook = rpcHookForAuth;
        this.allocateMessageQueueStrategy = new AllocateMessageQueueByIDC();

        defaultMQPushConsumer = new DefaultMQPushConsumer(eventBusClientConfig.getConsumerGroup(), rpcHook, allocateMessageQueueStrategy);
        defaultMQPushConsumer.setVipChannelEnabled(false);
    }

    /**
     * start the consumer which will begin to connect with the broker and then message can be consumed.
     * If the consumer has been already started, nothing will happen.
     * @throws MQClientException
     */
    public void start() throws MQClientException {
        if (isStart.compareAndSet(false, true)) {

            if (eventBusClientConfig.getNamesrvAddr() != null) {
                this.defaultMQPushConsumer.setNamesrvAddr(eventBusClientConfig.getNamesrvAddr());
            }
            this.defaultMQPushConsumer.changeInstanceNameToPID();

            String instanceName = this.defaultMQPushConsumer.getInstanceName() + EventBusConstant.INSTANCE_NAME_SEPERATER + EventBusVersion.getVersionDesc(eventBusClientConfig.getVersion());
            if (eventBusClientConfig.getClusterPrefix() != null) {
                instanceName = instanceName + EventBusConstant.INSTANCE_NAME_SEPERATER + eventBusClientConfig.getClusterPrefix();
            }
            this.defaultMQPushConsumer.setInstanceName(instanceName);
            defaultMQPushConsumer.setConsumeMessageBatchMaxSize(eventBusClientConfig.getConsumeMessageBatchMaxSize());
            defaultMQPushConsumer.setPullInterval(eventBusClientConfig.getPullInterval());
            defaultMQPushConsumer.setPullBatchSize(eventBusClientConfig.getPullBatchSize());
            defaultMQPushConsumer.setConsumeConcurrentlyMaxSpan(eventBusClientConfig.getConsumeConcurrentlyMaxSpan());
            defaultMQPushConsumer.setPollNameServerInterval(eventBusClientConfig.getPollNameServerInterval());
            defaultMQPushConsumer.setPullThresholdForQueue(eventBusClientConfig.getAckWindowSize());
            defaultMQPushConsumer.setConsumeTimeout(eventBusClientConfig.getConsumeTimeout());
            defaultMQPushConsumer.setConsumeThreadMax(eventBusClientConfig.getThreadPoolMaxSize());
            defaultMQPushConsumer.setConsumeThreadMin(eventBusClientConfig.getThreadPoolCoreSize());
            defaultMQPushConsumer.setPersistConsumerOffsetInterval(eventBusClientConfig.getAckTime());
            defaultMQPushConsumer.setMaxReconsumeTimes(eventBusClientConfig.getMaxReconsumeTimes());
            defaultMQPushConsumer.setHeartbeatBrokerInterval(eventBusClientConfig.getHeartbeatBrokerInterval());

            eventBusClientInstance = EventBusClientManager.getInstance().getAndCreateEventBusClientInstance(defaultMQPushConsumer, rpcHook);

            eventBusClientInstance.start();

            if (allocateMessageQueueStrategy instanceof AllocateMessageQueueByIDC) {
                ((AllocateMessageQueueByIDC) allocateMessageQueueStrategy).setMqClientInstance(eventBusClientInstance);
            }

            if (eventBusClientConfig.getWsAddr() != null) {
                EventBusClientAPIImpl eventClientAPI = (EventBusClientAPIImpl) eventBusClientInstance.getMQClientAPIImpl();
                eventClientAPI.setWsAddr(eventBusClientConfig.getWsAddr());
                eventClientAPI.fetchNameServerAddr();
            }

            this.defaultMQPushConsumer.start();
            final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
            defaultMQPushConsumer.unsubscribe(retryTopic);
            defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer();

            LOG.info("EventBusPushConsumer start ok");
        } else {
            LOG.warn("EventBusPushConsumer already started");
        }
    }

    public void shutdown() {
        if (isStart.compareAndSet(true, false)) {
            this.defaultMQPushConsumer.shutdown();
            LOG.info("EventBusPushConsumer [{}] shutdown", defaultMQPushConsumer.getInstanceName());
        } else {
            LOG.info("EventBusPushConsumer [{}] already shutdown", defaultMQPushConsumer.getInstanceName());
        }
    }

    /**
     * register a message listener which specify the callback message how message should be consumed. The message will be consumed in a standalone thread pool.
     * @param messageListener
     */
    public void registerMessageListener(MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumer.registerMessageListener(messageListener);
    }

    /**
     * subscirbe a topic so that the consumer can consume message from. Typically, you should subscribe topic first then start the consumer
     * @param topic topic name that the consumer needs to subscribe
     * @throws MQClientException
     */
    public void subscribe(String topic) throws MQClientException {
        this.defaultMQPushConsumer.subscribe(topic, "*");
        LOG.info("add subscription [{}] to consumer", topic);
    }

    public void unsubscribe(String topic) {
        unsubscribe(topic, true);
    }

    public void unsubscribe(String topic, boolean isNeedSendHeartbeat) {
        LOG.info("remove subscription [{}] from consumer", topic);
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable =
            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getRebalanceImpl().getProcessQueueTable();

        for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
            MessageQueue messageQueue = entry.getKey();
            ProcessQueue pq = entry.getValue();
            if (messageQueue.getTopic().equals(topic)) {
                pq.setDropped(true);
            }
        }
        this.defaultMQPushConsumer.unsubscribe(topic);
        if (isStart.get()) {
            if (isNeedSendHeartbeat) {
                sendHeartBeatToBrokersWhenSubscribeChange();
            }
        }
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public EventBusClientInstance getEventBusClientInstance() {
        return eventBusClientInstance;
    }

    public String getNamesrvAddr() {
        return this.defaultMQPushConsumer.getNamesrvAddr();
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.defaultMQPushConsumer.setNamesrvAddr(namesrvAddr);
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.defaultMQPushConsumer.setConsumeFromWhere(consumeFromWhere);
    }

    private void sendHeartBeatToBrokersWhenSubscribeChange() {
        this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
    }
}
