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

package com.gcote.eventbus.client.impl.producer;

import com.gcote.eventbus.client.common.EventBusClientConfig;
import com.gcote.eventbus.client.impl.factory.EventBusClientInstance;
import com.gcote.eventbus.common.EventBusConstant;
import com.gcote.eventbus.common.EventBusErrorCode;
import com.gcote.eventbus.common.exception.EventBusException;
import com.gcote.eventbus.common.protocol.EventBusResponseCode;
import com.gcote.eventbus.common.util.EventBusRequestIDUtil;
import com.gcote.eventbus.producer.EventBusProducer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class EventBusProducerImpl {
    public static final Logger LOGGER = LoggerFactory.getLogger(EventBusProducerImpl.class);

    private EventBusProducer eventBusProducer;
    private HealthyMessageQueueSelector messageQueueSelector;
    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService executorService = null;
    private ConcurrentHashMap<String, Boolean> topicInitMap = new ConcurrentHashMap<String, Boolean>();
    private ClusterInfo clusterInfo;

    public EventBusProducerImpl(EventBusProducer eventBusProducer, EventBusClientConfig eventBusClientConfig,
                                EventBusClientInstance eventBusClientInstance) {
        this.eventBusProducer = eventBusProducer;
        this.messageQueueSelector = new HealthyMessageQueueSelector(new MessageQueueHealthManager(eventBusClientConfig.getQueueIsolateTimeMillis()),
                eventBusClientConfig.getMinMqNumWhenSendLocal());

        executorService = eventBusClientInstance.getExecutorService();
        scheduledExecutorService = eventBusClientInstance.getScheduledExecutorService();

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanExpiredRRRequest();
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

    }

    private void cleanExpiredRRRequest() {
        try {
            List<RRResponseFuture> expiredRRRequest = new ArrayList<RRResponseFuture>();

            Iterator<Map.Entry<String, RRResponseFuture>> it = ResponseTable.getRrResponseFurtureConcurrentHashMap().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, RRResponseFuture> entry = it.next();
                String rId = entry.getKey();
                RRResponseFuture responseFurture = entry.getValue();
                if (responseFurture.getExpiredTime() + 1000L <= System.currentTimeMillis()) {
                    it.remove();
                    expiredRRRequest.add(responseFurture);
                    LOGGER.warn("remove timeout request " + rId);
                }
            }

            for (final RRResponseFuture responseFuture : expiredRRRequest) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (!responseFuture.release()) {
                            Throwable throwable = new EventBusException(EventBusErrorCode.RR_REQUEST_TIMEOUT, "remove timeout request, deadline: " + responseFuture.getExpiredTime());
                            responseFuture.getRrCallback().onException(throwable);
                        }
                    }
                });
            }
        } catch (Throwable ignore) {
            LOGGER.warn("cleanExpiredRRRequest failed ,{}", ignore.getMessage());
        }
    }

    public void reply(
        Message replyMsg,
        final SendCallback sendCallback) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        replyMsg.putUserProperty(EventBusConstant.KEY, EventBusConstant.REPLY);
        replyMsg.putUserProperty(EventBusConstant.PROPERTY_MESSAGE_TTL, String.valueOf(eventBusProducer.getDefaultMQProducer().getSendMsgTimeout()));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reply message: {} ", replyMsg.toString());
        }
        final String requestId = replyMsg.getUserProperty(EventBusConstant.PROPERTY_RR_REQUEST_ID);
        if (requestId == null) {
            LOGGER.warn("rr request id is null, can not reply");
        }
        publish(replyMsg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                if (sendCallback != null) {
                    sendCallback.onSuccess(sendResult);
                }
            }

            @Override
            public void onException(Throwable e) {
                LOGGER.warn("Reply message fail, requestId={}", requestId);
                if (sendCallback != null) {
                    sendCallback.onException(e);
                }
            }
        });
    }

    public Message request(Message requestMsg,
        long timeout) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return request(requestMsg, null, null, timeout);
    }

    public Message request(Message requestMsg, final SendCallback sendCallback, RRCallback rrCallback, long timeout)
        throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        boolean isAsyncRR = (rrCallback != null);

        final String uniqueRequestId = EventBusRequestIDUtil.createUniqueName("w");
        DefaultMQProducer producer = eventBusProducer.getDefaultMQProducer();
        requestMsg.putUserProperty(EventBusConstant.KEY, EventBusConstant.PERSISTENT);
        requestMsg.putUserProperty(EventBusConstant.PROPERTY_RR_REQUEST_ID, uniqueRequestId);
        requestMsg.putUserProperty(EventBusConstant.PROPERTY_MESSAGE_REPLY_TO, producer.buildMQClientId());
        requestMsg.putUserProperty(EventBusConstant.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        final RRResponseFuture responseFurture = new RRResponseFuture(rrCallback, timeout);

        String topic = requestMsg.getTopic();
        boolean hasRouteData = eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getTopicRouteTable().containsKey(topic);
        Boolean isSendHeartbeatOk = topicInitMap.get(topic);
        if (isSendHeartbeatOk == null) {
            isSendHeartbeatOk = false;
        }
        if (!hasRouteData || !isSendHeartbeatOk) {
            long startTimestamp = System.currentTimeMillis();
            synchronized (this) {
                boolean hasRouteDataSync = eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getTopicRouteTable().containsKey(topic);
                if (!hasRouteDataSync) {
                    LOGGER.info("no topic route info for " + topic + ", send heartbeat to nameserver");
                    eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
                    eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
                    topicInitMap.put(topic, true);
                }
            }
            long cost = System.currentTimeMillis() - startTimestamp;
            if (cost > 500) {
                LOGGER.warn("get topic route info for {} before request cost {} ms.", topic, cost);
            }
        }

        ResponseTable.getRrResponseFurtureConcurrentHashMap().put(uniqueRequestId, responseFurture);
        if (isAsyncRR) {
            this.publish(requestMsg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    if (sendCallback != null) {
                        sendCallback.onSuccess(sendResult);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    LOGGER.warn("except when publish async rr message, uniqueId :{} {} ", uniqueRequestId, e.getMessage());
                    ResponseTable.getRrResponseFurtureConcurrentHashMap().remove(uniqueRequestId);
                    if (sendCallback != null) {
                        sendCallback.onException(e);
                    }
                }
            }, timeout);
            return null;

        } else {
            publish(requestMsg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    if (sendCallback != null) {
                        sendCallback.onSuccess(sendResult);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    LOGGER.warn("except when publish sync rr message, uniqueId :{} {}", uniqueRequestId, e.getMessage());
                    ResponseTable.getRrResponseFurtureConcurrentHashMap().remove(uniqueRequestId);
                    if (sendCallback != null) {
                        sendCallback.onException(e);
                    }
                }
            }, timeout);
            Message retMessage = responseFurture.waitResponse(timeout);
            ResponseTable.getRrResponseFurtureConcurrentHashMap().remove(uniqueRequestId);
            if (retMessage == null) {
                LOGGER.warn("request {} is sent, constant is :{}, but no rr response ", topic, uniqueRequestId);
            }
            return retMessage;
        }
    }

    public void publish(Message msg) throws MQClientException, RemotingException, InterruptedException {
        publish(msg, eventBusProducer.getDefaultMQProducer().getSendMsgTimeout());
    }

    public void publish(Message msg, long timeout) throws MQClientException, RemotingException, InterruptedException {
        publish(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                LOGGER.debug(sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                LOGGER.warn("", e);
            }
        }, timeout);
    }

    public void publish(
        Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        for (Message msg : msgs) {
            if (msg.getUserProperty(EventBusConstant.PROPERTY_MESSAGE_TTL) == null) {
                msg.putUserProperty(EventBusConstant.PROPERTY_MESSAGE_TTL, EventBusConstant.DEFAULT_TTL);
            }
        }
        publish(batch(msgs));
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, eventBusProducer.getDefaultMQProducer());
                MessageClientIDSetter.setUniqID(message);
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        return msgBatch;
    }

    public void publish(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        publish(msg, sendCallback, this.eventBusProducer.getDefaultMQProducer().getSendMsgTimeout());
    }

    public void publish(final Message msg, final SendCallback sendCallback,
        final long timeout) throws MQClientException, RemotingException, InterruptedException {
        if (msg.getUserProperty(EventBusConstant.PROPERTY_MESSAGE_TTL) == null) {
            msg.putUserProperty(EventBusConstant.PROPERTY_MESSAGE_TTL, EventBusConstant.DEFAULT_TTL);
        }

        final AtomicReference<MessageQueue> selectorArgs = new AtomicReference<MessageQueue>();
        AsynCircuitBreakSendCallBack asynCircuitBreakSendCallBack = new AsynCircuitBreakSendCallBack();
        asynCircuitBreakSendCallBack.setMsg(msg);
        asynCircuitBreakSendCallBack.setProducer(this.eventBusProducer);
        asynCircuitBreakSendCallBack.setSelectorArg(selectorArgs);
        asynCircuitBreakSendCallBack.setSendCallback(sendCallback);

        String topic = msg.getTopic();
        boolean hasRouteData = eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getTopicRouteTable().containsKey(topic);
        if (!hasRouteData) {
            LOGGER.info("no topic route info for " + topic + ", send heartbeat to nameserver");
            eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
        }

        EventBusProducerImpl.this.eventBusProducer.getDefaultMQProducer().send(msg, messageQueueSelector, selectorArgs, asynCircuitBreakSendCallBack, timeout);
    }

    class AsynCircuitBreakSendCallBack implements SendCallback {
        private Message msg;
        private EventBusProducer producer;
        private AtomicReference<MessageQueue> selectorArg;
        private SendCallback sendCallback;
        private AtomicInteger sendRetryTimes = new AtomicInteger(0);
        private AtomicInteger circuitBreakRetryTimes = new AtomicInteger(0);
        private int queueCount = 0;

        public void setProducer(EventBusProducer producer) {
            this.producer = producer;
        }

        public void setMsg(Message msg) {
            this.msg = msg;
        }

        public void setSelectorArg(AtomicReference<MessageQueue> selectorArg) {
            this.selectorArg = selectorArg;
        }

        public void setSendCallback(SendCallback sendCallback) {
            this.sendCallback = sendCallback;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
            messageQueueSelector.getMessageQueueHealthManager().markQueueHealthy(sendResult.getMessageQueue());
            if (sendCallback != null) {
                sendCallback.onSuccess(sendResult);
            }
        }

        @Override
        public void onException(Throwable e) {
            try {
                MessageQueueHealthManager messageQueueHealthManager
                    = ((HealthyMessageQueueSelector) messageQueueSelector).getMessageQueueHealthManager();
                MessageQueue messageQueue = ((AtomicReference<MessageQueue>) selectorArg).get();
                if (messageQueue != null) {
                    messageQueueSelector.getMessageQueueHealthManager().markQueueFault(messageQueue);
                    if (messageQueueSelector.getMessageQueueHealthManager().isQueueFault(messageQueue)) {
                        LOGGER.warn("isolate send failed mq. {} cause: {}", messageQueue, e.getMessage());
                    }
                }
                //logic of fuse
                if (e.getMessage().contains("CODE: " + EventBusResponseCode.CONSUME_DIFF_SPAN_TOO_LONG)) {
                    //first retry initialize
                    if (queueCount == 0) {
                        List<MessageQueue> messageQueueList = producer.getDefaultMQProducer().getDefaultMQProducerImpl().getTopicPublishInfoTable()
                            .get(msg.getTopic()).getMessageQueueList();
                        queueCount = messageQueueList.size();
                        String clusterPrefix = eventBusProducer.getEventBusClientConfig().getClusterPrefix();
                        if (!StringUtils.isEmpty(clusterPrefix)) {
                            for (MessageQueue mq : messageQueueList) {
                                if (messageQueueHealthManager.isQueueFault(mq)) {
                                    queueCount--;
                                }
                            }
                        }
                    }

                    int retryTimes = Math.min(queueCount, eventBusProducer.getEventBusClientConfig().getRetryTimesWhenSendAsyncFailed());
                    if (circuitBreakRetryTimes.get() < retryTimes) {
                        circuitBreakRetryTimes.incrementAndGet();
                        LOGGER.warn("fuse:send to [{}] circuit break, retry no.[{}] times, msgKey:[{}]", messageQueue.toString(), circuitBreakRetryTimes.intValue(), msg.getKeys());
                        producer.getDefaultMQProducer().send(msg, messageQueueSelector, selectorArg, this);
                        //no exception to client when retry
                        return;
                    } else {
                        LOGGER.warn("fuse:send to [{}] circuit break after retry {} times, msgKey:[{}]", messageQueue.toString(), retryTimes, msg.getKeys());
                    }
                } else {
                    int maxRetryTimes = producer.getEventBusClientConfig().getRetryTimesWhenSendAsyncFailed();
                    if (sendRetryTimes.getAndIncrement() < maxRetryTimes) {
                        LOGGER.info("send message fail, retry {} now, msgKey: {}, cause: {}", sendRetryTimes.get(), msg.getKeys(), e.getMessage());
                        producer.getDefaultMQProducer().send(msg, messageQueueSelector, selectorArg, this);
                        return;
                    } else {
                        LOGGER.warn("send message fail, after retry {} times, msgKey:[{}]", maxRetryTimes, msg.getKeys());
                    }
                }

                if (sendCallback != null) {
                    sendCallback.onException(e);
                }
            } catch (Exception e1) {
                LOGGER.warn("onExcept fail", e1);
                if (sendCallback != null) {
                    sendCallback.onException(e);
                }
            }
        }
    }

    public void updateSendNearbyMapping(Map<String, Boolean> newMapping) {
        this.messageQueueSelector.setSendNearbyMapping(newMapping);
    }

    public void startUpdateClusterInfoTask() {
        updateClusterInfo();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                updateClusterInfo();
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    private void updateClusterInfo() {
        try {
            MQClientInstance mqClientInstance = this.eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory();
            if (mqClientInstance != null
                && this.eventBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState() == ServiceState.RUNNING) {
                if (mqClientInstance.getMQClientAPIImpl() != null && mqClientInstance.getMQClientAPIImpl().getNameServerAddressList() != null
                    && mqClientInstance.getMQClientAPIImpl().getNameServerAddressList().size() == 0) {
                    mqClientInstance.getMQClientAPIImpl().fetchNameServerAddr();
                }
                clusterInfo = mqClientInstance.getMQClientAPIImpl().getBrokerClusterInfo(3000);
                updateLocalBrokers(clusterInfo);
            }
        } catch (Throwable e) {
            LOGGER.warn("updateClusterInfo failed, {}", e.getMessage());
        }
    }

    private void updateLocalBrokers(ClusterInfo clusterInfo) {
        if (clusterInfo != null) {
            String clusterPrefix = eventBusProducer.getEventBusClientConfig().getClusterPrefix();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> currentBrokers = new HashSet<String>();
            for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                String clusterName = entry.getKey();
                String clusterIdc = StringUtils.split(clusterName, EventBusConstant.IDC_SEPERATER)[0];
                if (StringUtils.isNotEmpty(clusterPrefix) && StringUtils.equalsIgnoreCase(clusterIdc, clusterPrefix)) {
                    currentBrokers.addAll(entry.getValue());
                }
            }
            if (!currentBrokers.equals(messageQueueSelector.getLocalBrokers())) {
                messageQueueSelector.setLocalBrokers(currentBrokers);
                LOGGER.info("localBrokers updated:  {} , clusterPrefix :{} ", currentBrokers, clusterPrefix);
            }
        }
    }
}
