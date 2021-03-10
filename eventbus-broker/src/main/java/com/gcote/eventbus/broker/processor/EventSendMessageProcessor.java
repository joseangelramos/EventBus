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
import com.gcote.eventbus.broker.client.EventProducerManager;
import com.gcote.eventbus.broker.consumequeue.ConsumeQueueManager;
import com.gcote.eventbus.broker.consumequeue.ConsumeQueueWaterMark;
import com.gcote.eventbus.broker.consumequeue.MessageRedirectManager;
import com.gcote.eventbus.common.EventBusConstant;
import com.gcote.eventbus.common.protocol.EventBusResponseCode;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EventSendMessageProcessor extends SendMessageProcessor {
    private final static Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private ConsumeQueueManager eventQueueManager = ConsumeQueueManager.onlyInstance();

    public EventSendMessageProcessor(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        String Topic = requestHeader.getTopic();
        int queueIdInt = requestHeader.getQueueId();
        if (eventQueueManager.getBrokerController().getEventBusBrokerConfig().isRejectSendWhenMaxDepth()
            && Topic != null
            && !Topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
            && !Topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)
            && !Topic.contains(EventBusConstant.RR_REPLY_TOPIC)
            && !Topic.startsWith(EventBusConstant.RMQ_SYS)) {
            long maxQueueDepth = eventQueueManager.getMaxQueueDepth(Topic);
            double highWatermark = eventQueueManager.getBrokerController().getEventBusBrokerConfig().getQueueDepthHighWatermark();
            ConsumeQueueWaterMark minConsumeQueueWaterMark
                = eventQueueManager.getMinAccumulated(Topic, queueIdInt);
            if (minConsumeQueueWaterMark != null) {
                long accumulate = minConsumeQueueWaterMark.getAccumulated();
                if (accumulate >= maxQueueDepth) {
                    if (System.currentTimeMillis() % 100 == 0) {
                        LOG.error("Quota exceed 100% for topic [{}] in queue [{}], current:[{}], max:[{}]", Topic, queueIdInt, accumulate, maxQueueDepth);
                    }
                    final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
                    response.setCode(EventBusResponseCode.CONSUME_DIFF_SPAN_TOO_LONG);
                    response.setRemark(" consume span too long, maybe has slow consumer, so send rejected");
                    return response;
                } else if (accumulate >= maxQueueDepth * highWatermark) {
                    if (System.currentTimeMillis() % 100 == 0) {
                        LOG.error("Quota exceed {}% for topic [{}] in queue [{}], current:[{}], max:[{}]", highWatermark * 100, Topic, queueIdInt, accumulate, maxQueueDepth);
                    }
                }
            }
        }
        if (RequestCode.SEND_MESSAGE_V2 == request.getCode() || RequestCode.SEND_MESSAGE == request.getCode()) {
            Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
            EventProducerManager eventProducerManager = (EventProducerManager) this.brokerController.getProducerManager();
            String sendId = properties.get(EventBusConstant.PROPERTY_MESSAGE_REPLY_TO);

            if (sendId != null && eventProducerManager.getClientChannel(sendId) == null) {
                ClientChannelInfo clientChannelInfo = new ClientChannelInfo(ctx.channel(), sendId, request.getLanguage(), request.getVersion());
                eventProducerManager.registerProducer(requestHeader.getProducerGroup(), clientChannelInfo);
            }
        }

        if (((EventBrokerController) brokerController).getEventBusBrokerConfig().isRedirectMessageEnable()) {
            switch (request.getCode()) {
                case RequestCode.CONSUMER_SEND_MSG_BACK:
                    break;
                default:
                    Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
                    String redirectFlag = properties.get(EventBusConstant.REDIRECT_FLAG);
                    //redirect message
                    MessageRedirectManager.RedirectResult redirectResult = ((EventBrokerController) brokerController).getMessageRedirectManager()
                        .redirectMessageToWhichQueue(requestHeader, redirectFlag);
                    switch (redirectResult.getStates()) {
                        case REDIRECT_OK:
                            log.debug("redirect message from queueId({}) to queueId({}), {}", requestHeader.getQueueId(), redirectResult.getRedirectQueueId(), requestHeader.getTopic());
                            changeQueueIdInRequest(request, redirectResult.getRedirectQueueId());
                            properties.put(EventBusConstant.REDIRECT, "true");
                            updateProperties(request, MessageDecoder.messageProperties2String(properties));
                            break;
                        case NO_REDIRECT_CONFIG:
                            properties.put(EventBusConstant.REDIRECT, "false");
                            updateProperties(request, MessageDecoder.messageProperties2String(properties));
                            break;
                        case NO_INSTANCE_FOUND:
                            RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
                            response.setCode(ResponseCode.SYSTEM_ERROR);
                            response.setRemark("Redirect instance no found for [" + requestHeader.getTopic() + "] in ["
                                + RemotingUtil.socketAddress2String(getStoreHost()) + "] redirect flag: " + redirectFlag);
                            return response;
                    }
            }
        }
        return super.processRequest(ctx, request);
    }

    private void changeQueueIdInRequest(final RemotingCommand request, int queueId) {
        switch (request.getCode()) {
            case RequestCode.SEND_BATCH_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
                request.getExtFields().put("e", String.valueOf(queueId));
            case RequestCode.SEND_MESSAGE:
                request.getExtFields().put("queueId", String.valueOf(queueId));
            default:
                break;
        }
    }

    private void updateProperties(final RemotingCommand request, String properties) {
        switch (request.getCode()) {
            case RequestCode.SEND_BATCH_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
                request.getExtFields().put("i", properties);
            case RequestCode.SEND_MESSAGE:
                request.getExtFields().put("properties", properties);
            default:
                break;
        }
    }
}
