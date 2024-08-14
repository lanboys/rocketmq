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
package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.example.utils.PrintUtil;

import java.util.List;

/**
 * 常用消费者客户端
 */
public class PushConsumer {

    public static void main(String[] args) throws InterruptedException, MQClientException {
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("a-group");
        consumer.setNamesrvAddr("localhost:19876");
        consumer.subscribe("aaaaa", "*");
        // consumer.subscribe("bbbbb", "*");// 订阅多个主题
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // wrong time format 2017_0422_221800
        // consumer.setConsumeTimestamp("20170422221800");
        consumer.setPullBatchSize(1);
        // consumer.setConsumeMessageBatchMaxSize(5);
        // consumer.setPullInterval(20000);
        consumer.setPullThresholdForQueue(100);
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                // System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

                System.out.println(Thread.currentThread().getName() + " 收到 " + msgs.size() + " 条消息");
                context.setAckIndex(-1);// 设置消费成功的索引
                for (int i = 0; i < msgs.size(); i++) {
                    try {
                        Thread.sleep(3000);
                        MessageExt msg = msgs.get(i);
                        // if (4 == 4) {
                        //     PrintUtil.printMessage(msg);
                        //     throw new RuntimeException("模拟消息消费失败");
                        // }
                        PrintUtil.printMessage(msg);
                        context.setAckIndex(i);// 设置消费成功的索引
                    } catch (Exception e) {
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }

                // return ConsumeConcurrentlyStatus.RECONSUME_LATER; 效果等于 ackIndex == -1
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.setNamesrvAddr("localhost:19876");
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
