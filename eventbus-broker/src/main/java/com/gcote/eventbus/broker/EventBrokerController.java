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

import com.gcote.eventbus.broker.client.AdjustQueueNumStrategy;
import com.gcote.eventbus.broker.client.EventConsumerManager;
import com.gcote.eventbus.broker.client.EventProducerManager;
import com.gcote.eventbus.broker.consumequeue.ClientRebalanceResultManager;
import com.gcote.eventbus.broker.consumequeue.ConsumeQueueManager;
import com.gcote.eventbus.broker.consumequeue.MessageRedirectManager;
import com.gcote.eventbus.broker.monitor.QueueListeningMonitor;
import com.gcote.eventbus.broker.net.EventBusBroker2Client;
import com.gcote.eventbus.broker.processor.*;
import com.gcote.eventbus.broker.topic.EventTopicConfigManager;
import com.gcote.eventbus.common.EventBusBrokerConfig;
import com.gcote.eventbus.common.EventBusConstant;
import com.gcote.eventbus.common.protocol.EventBusRequestCode;
import com.gcote.eventbus.common.util.ReflectUtil;
import org.apache.commons.lang3.Validate;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class EventBrokerController extends BrokerController {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final Logger LOG_WATER_MARK = LoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);

    private final EventProducerManager producerManager;
    private final EventConsumerManager consumerManager;
    private final EventBusBroker2Client eventBusBroker2Client;
    private final ExecutorService eventManageExecutor;
    private final ExecutorService sendReplyMessageExecutor;
    private final ExecutorService pushReplyMessageExecutor;
    private final BlockingQueue<Runnable> sendReplyThreadPoolQueue;
    private final BlockingQueue<Runnable> pushReplyThreadPoolQueue;
    private final ScheduledExecutorService eventScheduledExecutorService;
    private final ScheduledThreadPoolExecutor sendReplyScheduledExecutorService;

    private RemotingServer fastRemotingServer = null;
    private final ConsumeQueueManager consumeQueueManager;
    private final EventBusBrokerConfig eventBusBrokerConfig;
    private final EventTopicConfigManager extTopicConfigManager;
    private final QueueListeningMonitor queueListeningMonitor;

    private EventPullMessageProcessor eventPullMessageProcessor;
    private MessageRedirectManager messageRedirectManager;
    private ClientRebalanceResultManager clientRebalanceResultManager;

    public EventBrokerController(BrokerConfig brokerConfig, NettyServerConfig nettyServerConfig,
                                 NettyClientConfig nettyClientConfig, MessageStoreConfig messageStoreConfig,
                                 EventBusBrokerConfig eventBusBrokerConfig) {
        super(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        producerManager = new EventProducerManager();

        ConsumerIdsChangeListener consumerIdsChangeListener = (ConsumerIdsChangeListener) ReflectUtil.getSimpleProperty(BrokerController.class, this, "consumerIdsChangeListener");
        AdjustQueueNumStrategy adjustQueueNumStrategy = new AdjustQueueNumStrategy(this);
        consumerManager = new EventConsumerManager(consumerIdsChangeListener, adjustQueueNumStrategy);

        this.eventManageExecutor =
            Executors.newFixedThreadPool(brokerConfig.getClientManageThreadPoolNums(), new ThreadFactoryImpl(
                "ClientManageThread_"));
        eventBusBroker2Client = new EventBusBroker2Client(this);
        this.sendReplyThreadPoolQueue = new LinkedBlockingQueue<Runnable>(eventBusBrokerConfig.getSendReplyThreadPoolQueueCapacity());
        this.pushReplyThreadPoolQueue = new LinkedBlockingQueue<Runnable>(eventBusBrokerConfig.getPushReplyThreadPoolQueueCapacity());

        this.sendReplyMessageExecutor = new ThreadPoolExecutor(//
            eventBusBrokerConfig.getSendReplyMessageThreadPoolNums(),//
            eventBusBrokerConfig.getSendReplyMessageThreadPoolNums(),//
            1000 * 60,//
            TimeUnit.MILLISECONDS,//
            this.sendReplyThreadPoolQueue,//
            new ThreadFactoryImpl("sendReplyMessageThread_"));
        this.pushReplyMessageExecutor = new ThreadPoolExecutor(//
            eventBusBrokerConfig.getPushReplyMessageThreadPoolNums(),//
            eventBusBrokerConfig.getPushReplyMessageThreadPoolNums(),//
            1000 * 60,//
            TimeUnit.MILLISECONDS,//
            this.pushReplyThreadPoolQueue,//
            new ThreadFactoryImpl("pushReplyMessageThread_"));

        this.consumeQueueManager = ConsumeQueueManager.onlyInstance();
        consumeQueueManager.setBrokerController(this);
        extTopicConfigManager = new EventTopicConfigManager(this);

        BrokerOuterAPI brokerOuterAPI = super.getBrokerOuterAPI();
        ReflectUtil.setSimpleProperty(BrokerController.class, this, "brokerOuterAPI", brokerOuterAPI);

        String wsAddr = eventBusBrokerConfig.getRmqAddressServerDomain() + "/" + eventBusBrokerConfig.getRmqAddressServerSubGroup();
        TopAddressing topAddressing = (TopAddressing) ReflectUtil.getSimpleProperty(BrokerOuterAPI.class, brokerOuterAPI, "topAddressing");
        ReflectUtil.setSimpleProperty(TopAddressing.class, topAddressing, "wsAddr", wsAddr);

        if (this.getBrokerConfig().getNamesrvAddr() != null) {
            brokerOuterAPI.updateNameServerAddressList(this.getBrokerConfig().getNamesrvAddr());
            LOG.info("user specfied name server address: {}", this.getBrokerConfig().getNamesrvAddr());
        }

        eventScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "brokerControllerScheduledThread");
                t.setDaemon(true);
                return t;
            }
        });

        sendReplyScheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "sendReplyScheduledThread");
                t.setDaemon(true);
                return t;
            }
        });

        this.eventBusBrokerConfig = eventBusBrokerConfig;
        this.getConfiguration().registerConfig(eventBusBrokerConfig);
        this.messageRedirectManager = new MessageRedirectManager(this);
        this.clientRebalanceResultManager = new ClientRebalanceResultManager(this);
        this.queueListeningMonitor = new QueueListeningMonitor(this);
        EventBusBrokerStartup.setEventBrokerController(this);
    }

    @Override
    public boolean initialize() throws CloneNotSupportedException {
        boolean result = super.initialize();

        result = result && this.extTopicConfigManager.load();

        //reset the lastDeliverOffsetTable as offsetTable of consumer
        consumeQueueManager.load();

        String rrTopic = this.getBrokerConfig().getBrokerClusterName() + "-" + EventBusConstant.RR_REPLY_TOPIC;
        if (getTopicConfigManager().selectTopicConfig(rrTopic) == null) {
            TopicConfig topicConfig = new TopicConfig(rrTopic);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setReadQueueNums(1);
            topicConfig.setPerm(PermName.PERM_INHERIT | PermName.PERM_READ | PermName.PERM_WRITE);
            this.getTopicConfigManager().updateTopicConfig(topicConfig);
        }
        this.getTopicConfigManager().getSystemTopic().add(rrTopic);

        return result;
    }

    public EventTopicConfigManager getExtTopicConfigManager() {
        return EventBrokerController.this.extTopicConfigManager;
    }

    public void start() throws Exception {
        super.start();
        eventScheduledExecutorService.scheduleAtFixedRate(() -> {
            this.getConsumerOffsetManager().scanUnsubscribedTopic();
            this.getConsumeQueueManager().scanUnsubscribedTopic();
        }, 3600 * 1000, 3600 * 1000, TimeUnit.MILLISECONDS);
        this.queueListeningMonitor.start();
    }

    public void scheduleTask(Runnable task, long delay) {
        eventScheduledExecutorService.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    public void scheduleTaskAtFixedRate(Runnable task, long delay, long period) {
        eventScheduledExecutorService.scheduleAtFixedRate(task, delay, period, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        eventManageExecutor.shutdown();
        sendReplyMessageExecutor.shutdown();
        pushReplyMessageExecutor.shutdown();

        eventScheduledExecutorService.shutdown();
        sendReplyScheduledExecutorService.shutdown();

        messageRedirectManager.shutdown();
        this.queueListeningMonitor.shutdown();

        super.shutdown();
    }

    public void registerProcessor() {
        super.registerProcessor();
        fastRemotingServer = (RemotingServer) ReflectUtil.getSimpleProperty(BrokerController.class, this, "fastRemotingServer");
        Validate.notNull(fastRemotingServer, "fastRemotingServer is null");

        EventReplyMessageProcessor sendDirectMessageProcessor = new EventReplyMessageProcessor(this);
        super.getRemotingServer().registerProcessor(EventBusRequestCode.SEND_DIRECT_MESSAGE, sendDirectMessageProcessor, this.sendReplyMessageExecutor);
        super.getRemotingServer().registerProcessor(EventBusRequestCode.SEND_DIRECT_MESSAGE_V2, sendDirectMessageProcessor, this.sendReplyMessageExecutor);
        fastRemotingServer.registerProcessor(EventBusRequestCode.SEND_DIRECT_MESSAGE, sendDirectMessageProcessor, this.sendReplyMessageExecutor);
        fastRemotingServer.registerProcessor(EventBusRequestCode.SEND_DIRECT_MESSAGE_V2, sendDirectMessageProcessor, this.sendReplyMessageExecutor);

        EventAdminBrokerProcessor extAdminBrokerProcessor = new EventAdminBrokerProcessor(this);
        ExecutorService adminBrokerExecutor = (ExecutorService) ReflectUtil.getSimpleProperty(BrokerController.class,
            this, "adminBrokerExecutor");
        super.getRemotingServer().registerProcessor(EventBusRequestCode.GET_CONSUME_STATS_V2, extAdminBrokerProcessor, adminBrokerExecutor);
        super.getRemotingServer().registerProcessor(RequestCode.GET_BROKER_RUNTIME_INFO, extAdminBrokerProcessor, adminBrokerExecutor);
        fastRemotingServer.registerProcessor(EventBusRequestCode.GET_CONSUME_STATS_V2, extAdminBrokerProcessor, adminBrokerExecutor);
        fastRemotingServer.registerProcessor(RequestCode.GET_BROKER_RUNTIME_INFO, extAdminBrokerProcessor, adminBrokerExecutor);
        super.getRemotingServer().registerProcessor(RequestCode.UPDATE_AND_CREATE_TOPIC, extAdminBrokerProcessor, adminBrokerExecutor);
        fastRemotingServer.registerProcessor(RequestCode.UPDATE_AND_CREATE_TOPIC, extAdminBrokerProcessor, adminBrokerExecutor);

        EventSendMessageProcessor eventSendMessageProcessor = new EventSendMessageProcessor(this);
        ExecutorService sendMessageExecutor = (ExecutorService) ReflectUtil.getSimpleProperty(BrokerController.class,
            this, "sendMessageExecutor");
        super.getRemotingServer().registerProcessor(RequestCode.SEND_MESSAGE, eventSendMessageProcessor, sendMessageExecutor);
        super.getRemotingServer().registerProcessor(RequestCode.SEND_MESSAGE_V2, eventSendMessageProcessor, sendMessageExecutor);
        super.getRemotingServer().registerProcessor(RequestCode.SEND_BATCH_MESSAGE, eventSendMessageProcessor, sendMessageExecutor);
        super.getRemotingServer().registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, eventSendMessageProcessor, sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, eventSendMessageProcessor, sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, eventSendMessageProcessor, sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, eventSendMessageProcessor, sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, eventSendMessageProcessor, sendMessageExecutor);

        eventPullMessageProcessor = new EventPullMessageProcessor(this);
        ExecutorService pullMessageExecutor = (ExecutorService) ReflectUtil.getSimpleProperty(BrokerController.class,
            this, "pullMessageExecutor");
        super.getRemotingServer().registerProcessor(RequestCode.PULL_MESSAGE, eventPullMessageProcessor, pullMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.PULL_MESSAGE, eventPullMessageProcessor, pullMessageExecutor);

        EventClientManageProcessor eventClientManageProcessor = new EventClientManageProcessor(this);
        super.getRemotingServer().registerProcessor(EventBusRequestCode.GET_CONSUMER_LIST_BY_GROUP_AND_TOPIC, eventClientManageProcessor, eventManageExecutor);
        fastRemotingServer.registerProcessor(EventBusRequestCode.GET_CONSUMER_LIST_BY_GROUP_AND_TOPIC, eventClientManageProcessor, eventManageExecutor);

    }

    public EventBusBroker2Client getEventBusBroker2Client() {
        return eventBusBroker2Client;
    }

    @Override
    public EventProducerManager getProducerManager() {
        return producerManager;
    }

    @Override
    public void printWaterMark() {
        LOG_WATER_MARK.info("{\"SendQueueSize\":\"{}\",\"PullQueueSize\":\"{}\",\"GotQueueSize\":\"{}\",\"PushQueueSize\":\"{}\",\"SendSlowTimeMills\":\"{}\",\"PullSlowTimeMills\":\"{}\",\"HeartbeatQueueSize\":\"{}\"}",
            this.getSendThreadPoolQueue().size(),
            this.getPullThreadPoolQueue().size(),
            this.sendReplyThreadPoolQueue.size(),
            this.pushReplyThreadPoolQueue.size(),
            this.headSlowTimeMills4SendThreadPoolQueue(),
            this.headSlowTimeMills4PullThreadPoolQueue(),
            this.getHeartbeatThreadPoolQueue().size());
    }

    public EventBusBrokerConfig getEventBusBrokerConfig() {
        return eventBusBrokerConfig;
    }

    public ExecutorService getPushReplyMessageExecutor() {
        return pushReplyMessageExecutor;
    }

    public ExecutorService getSendReplyMessageExecutor() {
        return sendReplyMessageExecutor;
    }

    public ScheduledThreadPoolExecutor getSendReplyScheduledExecutorService() {
        return sendReplyScheduledExecutorService;
    }

    public ConsumeQueueManager getConsumeQueueManager() {
        return consumeQueueManager;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return eventPullMessageProcessor;
    }

    @Override
    public EventConsumerManager getConsumerManager() {
        return this.consumerManager;
    }

    public MessageRedirectManager getMessageRedirectManager() {
        return messageRedirectManager;
    }

    public ClientRebalanceResultManager getClientRebalanceResultManager() {
        return clientRebalanceResultManager;
    }
}
