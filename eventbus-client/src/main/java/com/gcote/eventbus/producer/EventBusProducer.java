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

package com.gcote.eventbus.producer;


import com.gcote.eventbus.client.EventBusClientManager;
import com.gcote.eventbus.client.common.EventBusClientConfig;
import com.gcote.eventbus.client.impl.EventBusClientAPIImpl;
import com.gcote.eventbus.client.impl.factory.EventBusClientInstance;
import com.gcote.eventbus.client.impl.hook.EventBusClientHookFactory;
import com.gcote.eventbus.client.impl.producer.EventBusProducerImpl;
import com.gcote.eventbus.client.impl.producer.RRCallback;
import com.gcote.eventbus.common.EventBusConstant;
import com.gcote.eventbus.common.EventBusVersion;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventBusProducer {
    private static final Logger LOG = LoggerFactory.getLogger(EventBusProducer.class);
    private DefaultMQProducer defaultMQProducer;
    private EventBusClientInstance eventBusClientInstance;
    private RPCHook rpcHook;
    private AtomicBoolean isStart = new AtomicBoolean(false);
    private EventBusProducerImpl eventBusProducerImpl;
    private EventBusClientConfig eventBusClientConfig;

    static {
        System.setProperty("rocketmq.client.log.loadconfig", "false");
    }

    public EventBusProducer() {
        this(new EventBusClientConfig());
    }

    public EventBusProducer(EventBusClientConfig eventBusClientConfig) {
        RPCHook rpcHookForAuth = EventBusClientHookFactory.createRPCHook(eventBusClientConfig.getRpcHook());
        defaultMQProducer = new DefaultMQProducer(eventBusClientConfig.getProducerGroup(), rpcHookForAuth);
        defaultMQProducer.setVipChannelEnabled(false);
        this.rpcHook = rpcHookForAuth;
        this.eventBusClientConfig = eventBusClientConfig;
    }

    /**
     * start the producer which will begin to connect with the broker. A producer MUST call this method before sending any message
     * If the producer has been already started, nothing will happen.
     * @throws MQClientException
     */
    public void start() throws MQClientException {
        if (isStart.compareAndSet(false, true)) {
            try {
                System.setProperty("com.rocketmq.remoting.clientAsyncSemaphoreValue", String.valueOf(eventBusClientConfig.getPubWindowSize()));

                if (eventBusClientConfig.getNamesrvAddr() != null) {
                    this.defaultMQProducer.setNamesrvAddr(eventBusClientConfig.getNamesrvAddr());
                }
                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }

                String instanceName = this.defaultMQProducer.getInstanceName() + EventBusConstant.INSTANCE_NAME_SEPERATER
                    + EventBusVersion.getVersionDesc(this.eventBusClientConfig.getVersion());

                if (eventBusClientConfig.getClusterPrefix() != null) {
                    instanceName = instanceName + EventBusConstant.INSTANCE_NAME_SEPERATER + eventBusClientConfig.getClusterPrefix();
                }
                this.defaultMQProducer.setInstanceName(instanceName);

                this.defaultMQProducer.setPollNameServerInterval(eventBusClientConfig.getPollNameServerInterval());
                this.defaultMQProducer.setRetryTimesWhenSendAsyncFailed(eventBusClientConfig.getRetryTimesWhenSendAsyncFailed());
                this.defaultMQProducer.setRetryTimesWhenSendFailed(eventBusClientConfig.getRetryTimesWhenSendFailed());
                this.defaultMQProducer.setHeartbeatBrokerInterval(eventBusClientConfig.getHeartbeatBrokerInterval());
                this.defaultMQProducer.setPersistConsumerOffsetInterval(eventBusClientConfig.getAckTime());
                eventBusClientInstance
                    = EventBusClientManager.getInstance().getAndCreateEventBusClientInstance(defaultMQProducer, rpcHook);

                if (eventBusClientConfig.getWsAddr() != null) {
                    EventBusClientAPIImpl eventClientAPI = (EventBusClientAPIImpl) eventBusClientInstance.getMQClientAPIImpl();
                    eventClientAPI.setWsAddr(eventBusClientConfig.getWsAddr());
                }

                eventBusProducerImpl = new EventBusProducerImpl(this, eventBusClientConfig, eventBusClientInstance);
                this.defaultMQProducer.start();
                eventBusProducerImpl.startUpdateClusterInfoTask();
            } catch (MQClientException e) {
                LOG.warn("EventBusProducer start client failed {}", e.getMessage());
                isStart.set(false);
                throw e;
            } catch (Exception e) {
                LOG.warn("EventBusProducer start client failed", e);
                isStart.set(false);
                throw new MQClientException("EventBusProducer start client failed", e);
            }

            MessageClientIDSetter.createUniqID();

            LOG.info("EventBusProducer start ok");
        } else {
            LOG.warn("EventBusProducer already started");
        }
    }

    public void shutdown() {
        if (isStart.compareAndSet(true, false)) {
            this.defaultMQProducer.shutdown();
            LOG.info("EventBusProducer [{}] shutdown", defaultMQProducer.getInstanceName());
        } else {
            LOG.info("EventBusProducer [{}] already shutdown", defaultMQProducer.getInstanceName());
        }
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    //sync Request-Response interface
    public Message request(Message msg, long timeout)
        throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return eventBusProducerImpl.request(msg, timeout);
    }

    //async Request-Response interface
    public void request(Message msg, RRCallback rrCallback, long timeout)
        throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        eventBusProducerImpl.request(msg, null, rrCallback, timeout);
    }

    public void request(Message msg, SendCallback sendCallback, RRCallback rrCallback, long timeout)
        throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        eventBusProducerImpl.request(msg, sendCallback, rrCallback, timeout);
    }

    public void reply(Message replyMsg, SendCallback sendCallback)
        throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        eventBusProducerImpl.reply(replyMsg, sendCallback);
    }

    //async publish with callback
    public void publish(Message msg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        this.eventBusProducerImpl.publish(msg, sendCallback);
    }

    //async public
    public void publish(Message msg) throws MQClientException, RemotingException, InterruptedException {
        this.eventBusProducerImpl.publish(msg);
    }

    public void publish(
        Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.eventBusProducerImpl.publish(msgs);
    }

    public String getNamesrvAddr() {
        return defaultMQProducer.getNamesrvAddr();
    }

    public void setNamesrvAddr(String namesrvAddr) {
        defaultMQProducer.setNamesrvAddr(namesrvAddr);
    }

    public boolean isStart() {
        return isStart.get();
    }

    public EventBusClientConfig getEventBusClientConfig() {
        return eventBusClientConfig;
    }

    public void updateSendNearbyMapping(Map<String, Boolean> newMapping) {
        this.eventBusProducerImpl.updateSendNearbyMapping(newMapping);
    }
}
