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

package com.gcote.eventbus.examples.simple;

import com.gcote.eventbus.client.common.EventBusClientConfig;
import com.gcote.eventbus.consumer.EventBusMessageListenerConcurrently;
import com.gcote.eventbus.consumer.EventBusPushConsumer;
import com.gcote.eventbus.producer.EventBusProducer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SubConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SubConsumer.class);

    public static void main(String[] args) throws MQClientException {
        String topic = "PublishTopic";
        EventBusClientConfig eventBusClientConfig = new EventBusClientConfig();
        eventBusClientConfig.setConsumerGroup("Your-group-name");
        eventBusClientConfig.setPullBatchSize(32);
        eventBusClientConfig.setThreadPoolCoreSize(12);
        eventBusClientConfig.setClusterPrefix("gcote");

        EventBusProducer eventBusProducer = new EventBusProducer(eventBusClientConfig);
        eventBusProducer.setNamesrvAddr("127.0.0.1:9876");
        eventBusProducer.start();

        EventBusPushConsumer eventBusPushConsumer = new EventBusPushConsumer(eventBusClientConfig);
        eventBusPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        eventBusPushConsumer.registerMessageListener(new EventBusMessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus handleMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    logger.info("begin handle: " + msg.toString());
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
