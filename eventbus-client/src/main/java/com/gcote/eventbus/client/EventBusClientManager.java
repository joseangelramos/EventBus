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

package com.gcote.eventbus.client;

import com.gcote.eventbus.client.impl.factory.EventBusClientInstance;
import com.gcote.eventbus.common.util.ReflectUtil;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.remoting.RPCHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class EventBusClientManager {
    private static EventBusClientManager instance = new EventBusClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    private ConcurrentHashMap<String/* clientId */, EventBusClientInstance> factoryTable;

    public static final Logger LOGGER = LoggerFactory.getLogger(EventBusClientManager.class);

    @SuppressWarnings("unchecked")
    private EventBusClientManager() {
        try {
            //factoryTable, shared by all producer and consumer
            //same clientId will return the same MQClientInstance
            //In order to set our own eventclient instance to all producer and consumer, need to get the pointer of this table
            factoryTable = (ConcurrentHashMap<String/* clientId */, EventBusClientInstance>) ReflectUtil.getSimpleProperty(MQClientManager.class,
                MQClientManager.getInstance(), "factoryTable");
        } catch (Exception e) {
            LOGGER.warn("failed to initialize factory in mqclient manager.", e);
        }
    }

    public static EventBusClientManager getInstance() {
        return instance;
    }

    public synchronized EventBusClientInstance getAndCreateEventBusClientInstance(final ClientConfig clientConfig,
                                                                                  RPCHook rpcHook) {

        String clientId = clientConfig.buildMQClientId();
        EventBusClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                new EventBusClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            EventBusClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                LOGGER.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                LOGGER.info("new instance activate. " + clientId);
            }
        }
        return instance;
    }
}
