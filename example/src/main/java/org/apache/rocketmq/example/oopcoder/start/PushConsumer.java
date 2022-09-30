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
package org.apache.rocketmq.example.oopcoder.start;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.example.utils.PrintUtil;

import java.util.List;

public class PushConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("c-group");
        consumer.setNamesrvAddr("localhost:19876");
        consumer.subscribe("aaaaa", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setPullBatchSize(1);

        consumer.registerMessageListener(new FailMessageListener());
        // consumer.registerMessageListener(new SuccessMessageListener());
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    public static class FailMessageListener implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            context.setAckIndex(-1);// 设置消费成功的索引
            for (MessageExt msg : msgs) {
                PrintUtil.printMessage(msg);
                sleep(5);
                // 直接返回失败
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                // ConsumeConcurrentlyStatus.RECONSUME_LATER 效果等于 ackIndex == -1
                // return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

    }

    public static class SuccessMessageListener implements MessageListenerConcurrently {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            context.setAckIndex(-1);// 设置消费初始索引
            for (int i = 0; i < msgs.size(); i++) {
                MessageExt msg = msgs.get(i);
                PrintUtil.printMessage(msg);
                sleep(10);
                context.setAckIndex(i);// 设置消费成功的索引
                System.out.println("consumeMessage(): 消费成功");
            }
            // return ConsumeConcurrentlyStatus.RECONSUME_LATER; 效果等于 ackIndex == -1
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

    }

    private static void sleep(long s) {
        try {
            Thread.sleep(s * 1000);
        } catch (InterruptedException e) {
            //  ignore
        }
    }
}
