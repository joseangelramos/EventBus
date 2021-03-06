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

package com.gcote.eventbus.common.protocol;

public class EventBusRequestCode {
    public static final int PUSH_RR_REPLY_MSG_TO_CLIENT = 400;
    public static final int SEND_DIRECT_MESSAGE = 402;
    public static final int SEND_DIRECT_MESSAGE_V2 = 403;
    public static final int GET_CONSUME_STATS_V2 = 506;
    public static final int GET_CONSUMER_LIST_BY_GROUP_AND_TOPIC = 507;
    public static final int NOTIFY_WHEN_TOPIC_CONFIG_CHANGE = 508;
}
