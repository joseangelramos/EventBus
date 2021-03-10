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

package com.gcote.eventbus.client.impl.factory;

import com.gcote.eventbus.client.impl.EventBusClientAPIImpl;
import com.gcote.eventbus.client.impl.EventBusClientRemotingProcessor;
import com.gcote.eventbus.client.impl.consumer.EventBusPullMessageService;
import com.gcote.eventbus.common.protocol.EventBusRequestCode;
import com.gcote.eventbus.common.util.ReflectUtil;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class EventBusClientInstance extends MQClientInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventBusClientInstance.class);
    private final ClientConfig clientConfig;
    private EventBusClientAPIImpl eventClientAPI;
    private ClientRemotingProcessor clientRemotingProcessor;
    private EventBusClientRemotingProcessor eventClientRemotingProcessor;
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;

    public EventBusClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public EventBusClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        super(clientConfig, instanceIndex, clientId, rpcHook);
        this.clientConfig = clientConfig;
        try {
            clientRemotingProcessor = (ClientRemotingProcessor) ReflectUtil.getSimpleProperty(MQClientInstance.class, this, "clientRemotingProcessor");

            eventClientRemotingProcessor = new EventBusClientRemotingProcessor(this);

            eventClientAPI = new EventBusClientAPIImpl(
                super.getNettyClientConfig(),
                clientRemotingProcessor,
                rpcHook,
                clientConfig);

            ReflectUtil.setSimpleProperty(MQClientInstance.class, this, "mQClientAPIImpl", eventClientAPI);

            EventBusPullMessageService eventBusPullMessageService = new EventBusPullMessageService(this);
            ReflectUtil.setSimpleProperty(MQClientInstance.class, this, "pullMessageService", eventBusPullMessageService);

            if (this.clientConfig.getNamesrvAddr() != null) {
                this.eventClientAPI.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
                LOGGER.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
            }

            executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "eventClientThread_" + this.threadIndex.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            });

            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "eventClientScheduledThread_");
                    t.setDaemon(true);
                    return t;
                }
            });

            super.getMQClientAPIImpl().getRemotingClient()
                .registerProcessor(EventBusRequestCode.PUSH_RR_REPLY_MSG_TO_CLIENT, eventClientRemotingProcessor, executorService);
            super.getMQClientAPIImpl().getRemotingClient()
                .registerProcessor(EventBusRequestCode.NOTIFY_WHEN_TOPIC_CONFIG_CHANGE, eventClientRemotingProcessor, executorService);
        } catch (Exception e) {
            LOGGER.warn("failed to initialize factory in mqclient manager.", e);
        }

    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        super.shutdown();
        this.executorService.shutdown();
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    @Override
    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                LOGGER.debug("findConsumerIdList of {} from broker {}", topic, brokerAddr);
                List<String> cidList = eventClientAPI.getConsumerIdListByGroupAndTopic(brokerAddr, group, topic, 3000);
                if (cidList != null && !cidList.isEmpty()) {
                    return cidList;
                }
            } catch (Exception e) {
                LOGGER.warn("getConsumerIdListByGroup failed, " + brokerAddr + " " + group + ", retry immediately");
            }

            String lastSelected = brokerAddr;
            brokerAddr = this.findAnotherBrokerAddrByTopic(topic, lastSelected);
            if (null == brokerAddr) {
                this.updateTopicRouteInfoFromNameServer(topic);
                brokerAddr = this.findAnotherBrokerAddrByTopic(topic, lastSelected);
            }
            if (null != brokerAddr) {
                try {
                    LOGGER.debug("findConsumerIdList of {} from broker {}", topic, brokerAddr);
                    List<String> cidList = eventClientAPI.getConsumerIdListByGroupAndTopic(brokerAddr, group, topic, 3000);
                    return cidList;
                } catch (Exception e) {
                    LOGGER.warn("getConsumerIdListByGroup failed, " + brokerAddr + " " + group + ", after retry ", e);
                }
            }
        }

        return null;
    }

    private String findAnotherBrokerAddrByTopic(String topic, String lastSelected) {
        TopicRouteData topicRouteData = this.getTopicRouteTable().get(topic);
        if (topicRouteData != null && topicRouteData.getBrokerDatas() != null) {
            List<BrokerData> allBrokers = topicRouteData.getBrokerDatas();
            for (BrokerData bd : allBrokers) {
                if (!bd.selectBrokerAddr().equals(lastSelected)) {
                    String addr = bd.selectBrokerAddr();
                    LOGGER.debug("find another broker addr by topic [{}], find addr: {}, lastSelected: {}", topic, addr, lastSelected);
                    return addr;
                }
            }

            if (!allBrokers.isEmpty()) {
                int index = RandomUtils.nextInt(0, allBrokers.size());
                BrokerData bd = allBrokers.get(index % allBrokers.size());
                String addr = bd.selectBrokerAddr();
                LOGGER.debug("find any broker addr by topic [{}], find addr: {}, lastSelected: {}", topic, addr, lastSelected);
                return addr;
            }
        }
        return null;
    }
}
