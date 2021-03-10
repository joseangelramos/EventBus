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

package com.gcote.eventbus.broker.net;

import com.gcote.eventbus.broker.EventBrokerController;
import com.gcote.eventbus.common.protocol.EventBusRequestCode;
import com.gcote.eventbus.common.protocol.header.NotifyTopicChangedRequestHeader;
import com.gcote.eventbus.common.protocol.header.ReplyMessageRequestHeader;
import io.netty.channel.Channel;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventBusBroker2Client {
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final EventBrokerController eventBrokerController;

    public EventBusBroker2Client(EventBrokerController eventBrokerController) {
        this.eventBrokerController = eventBrokerController;
    }

    public boolean pushRRReplyMessageToClient(final Channel channel,
        ReplyMessageRequestHeader replyMessageRequestHeader, MessageExt msgInner) {
        replyMessageRequestHeader.setSysFlag(msgInner.getSysFlag());
        RemotingCommand request = RemotingCommand.createRequestCommand(EventBusRequestCode.PUSH_RR_REPLY_MSG_TO_CLIENT, replyMessageRequestHeader);
        request.markOnewayRPC();
        request.setBody(msgInner.getBody());
        try {
            this.eventBrokerController.getRemotingServer().invokeOneway(channel, request, 3000);
        } catch (RemotingTimeoutException e) {
            LOG.warn("push reply message to client failed ", e);
            try {
                this.eventBrokerController.getRemotingServer().invokeOneway(channel, request, 3000);
            } catch (Exception sube) {
                LOG.warn("push reply message to client failed again ", sube);
                return false;
            }
        } catch (Exception e) {
            LOG.warn("push reply message to client failed ", e);
            return false;
        }

        return true;
    }

    public void notifyWhenTopicConfigChange(final Channel channel, String topic) {
        NotifyTopicChangedRequestHeader notifyTopicChangedRequestHeader = new NotifyTopicChangedRequestHeader();
        notifyTopicChangedRequestHeader.setTopic(topic);
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(EventBusRequestCode.NOTIFY_WHEN_TOPIC_CONFIG_CHANGE, notifyTopicChangedRequestHeader);
        remotingCommand.markOnewayRPC();
        try {
            this.eventBrokerController.getRemotingServer().invokeOneway(channel, remotingCommand, 500);
        } catch (Exception e) {
            LOG.warn("notify consumer <" + channel + "> topic config change fail.", e);
        }
    }
}
