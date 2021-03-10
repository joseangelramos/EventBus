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

package com.gcote.eventbus.examples.rpc;

import com.gcote.eventbus.client.common.EventBusClientConfig;
import com.gcote.eventbus.client.common.EventBusClientUtil;
import com.gcote.eventbus.common.EventBusConstant;
import com.gcote.eventbus.consumer.EventBusMessageListenerConcurrently;
import com.gcote.eventbus.consumer.EventBusPushConsumer;
import com.gcote.eventbus.producer.EventBusProducer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ResponseConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ResponseConsumer.class);

    public static void main(String[] args) throws MQClientException {
        String topic = "RequestTopic";
        EventBusClientConfig eventBusClientConfig = new EventBusClientConfig();
        eventBusClientConfig.setConsumerGroup("Your-group-name");
        eventBusClientConfig.setPullBatchSize(32);
        eventBusClientConfig.setThreadPoolCoreSize(12);
        eventBusClientConfig.setClusterPrefix("XL");
        EventBusProducer eventBusProducer = new EventBusProducer(eventBusClientConfig);
        eventBusProducer.setNamesrvAddr("127.0.0.1:9876");
        eventBusProducer.start();
        EventBusPushConsumer eventBusPushConsumer = new EventBusPushConsumer(eventBusClientConfig);
        eventBusPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        eventBusPushConsumer.registerMessageListener(new EventBusMessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus handleMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String uniqueId = msg.getUserProperty(EventBusConstant.PROPERTY_RR_REQUEST_ID);
                    if (uniqueId == null) {
                        logger.info("REQUEST_ID is null from the request msg, will not reply this constant...");
                    } else {
                        try {
                            logger.info("begin handle: " + msg.toString());
                            Message replyMsg = EventBusClientUtil.createReplyMessage(msg, ("I am replying content").getBytes());
                            eventBusProducer.reply(replyMsg, new SendCallback() {
                                @Override
                                public void onSuccess(SendResult sendResult) {
                                    logger.info("reply success. {}", msg.toString());
                                }
                                @Override
                                public void onException(Throwable e) {
                                    logger.info("reply fail. {}", msg.toString(), e);
                                }
                            });
                        } catch (InterruptedException | RemotingException | MQClientException | MQBrokerException e) {
                            logger.warn("{}", e);
                        }
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        eventBusPushConsumer.subscribe(topic);
        eventBusPushConsumer.start();

        //shutdown the consumer when application exits.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                eventBusPushConsumer.shutdown();
            }
        });
    }
}
