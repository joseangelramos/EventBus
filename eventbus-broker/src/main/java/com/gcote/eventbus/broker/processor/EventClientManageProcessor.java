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

package com.gcote.eventbus.broker.processor;

import com.gcote.eventbus.broker.EventBrokerController;
import com.gcote.eventbus.broker.client.EventConsumerGroupInfo;
import com.gcote.eventbus.broker.client.EventConsumerManager;
import com.gcote.eventbus.common.protocol.EventBusRequestCode;
import com.gcote.eventbus.common.protocol.header.GetConsumerListByGroupAndTopicRequestHeader;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EventClientManageProcessor implements NettyRequestProcessor {
    private final EventBrokerController eventBrokerController;
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public EventClientManageProcessor(EventBrokerController eventBrokerController) {
        this.eventBrokerController = eventBrokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case EventBusRequestCode.GET_CONSUMER_LIST_BY_GROUP_AND_TOPIC:
                return getConsumerListByGroupAndTopic(ctx, request);
            default:
                break;
        }
        return null;
    }

    private RemotingCommand getConsumerListByGroupAndTopic(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        final GetConsumerListByGroupAndTopicRequestHeader requestHeader =
            (GetConsumerListByGroupAndTopicRequestHeader) request
                .decodeCommandCustomHeader(GetConsumerListByGroupAndTopicRequestHeader.class);
        EventConsumerManager eventConsumerManager = (EventConsumerManager) this.eventBrokerController.getConsumerManager();
        ConsumerGroupInfo consumerGroupInfo = eventConsumerManager.getConsumerGroupInfo(requestHeader.getConsumerGroup());

        if (consumerGroupInfo != null) {
            if (consumerGroupInfo instanceof EventConsumerGroupInfo) {
                EventConsumerGroupInfo wqCGInfo = (EventConsumerGroupInfo) consumerGroupInfo;
                List<String> cidList = new ArrayList<>();
                if (requestHeader.getTopic() != null) {
                    Set<String> cids = wqCGInfo.getClientIdBySubscription(requestHeader.getTopic());
                    if (cids != null) {
                        cidList.addAll(cids);
                    }
                    GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                    body.setConsumerIdList(cidList);
                    response.setBody(body.encode());
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    return response;
                }
            }

            //topic is null or consumerGroupInfo not an instance fo eventConsumerGroupInfo
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);
                response.setBody(body.encode());
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            } else {
                LOG.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            LOG.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}

