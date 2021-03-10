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
import com.gcote.eventbus.producer.EventBusProducer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubProducer {
    private static final Logger logger = LoggerFactory.getLogger(PubProducer.class);

    public static void main(String[] args) throws MQClientException {
        EventBusClientConfig clientConfig = new EventBusClientConfig();
        clientConfig.setClusterPrefix("XL");

        EventBusProducer eventBusProducer = new EventBusProducer(clientConfig);
        eventBusProducer.setNamesrvAddr("127.0.0.1:9876");
        eventBusProducer.start();

        String topic = "PublishTopic";
        final String content = "Fierro pariente";
        Message msg = new Message(topic, content.getBytes());
        try {
            eventBusProducer.publish(msg);
        } catch (MQClientException | RemotingException | InterruptedException e) {
            logger.warn("{}", e);
        } finally {
            // normally , we only shutdown EventBusProducer when the application exits. In this sample, we shutdown the producer when message is sent.
                    eventBusProducer.shutdown();
            logger.warn("Cerrado");
        }
    }
}
