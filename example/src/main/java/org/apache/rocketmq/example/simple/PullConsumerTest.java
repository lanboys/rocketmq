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

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class PullConsumerTest {

  public static void main(String[] args) throws MQClientException, InterruptedException {
    DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("a-group");
    consumer.setNamesrvAddr("localhost:19876");
    consumer.start();

    MessageQueue mq = new MessageQueue();
    mq.setQueueId(0);
    mq.setTopic("aaaaa");
    mq.setBrokerName("standalone-broker");

    long offset = 3;
    while (true) {
      try {
        long beginTime = System.currentTimeMillis();
        // 自己手动同步请求去拉
        PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, offset, 3);
        System.out.printf("%s%n", System.currentTimeMillis() - beginTime);
        System.out.printf("%s%n", pullResult);
        offset = pullResult.getNextBeginOffset();
        // 自己更新offset 可以试试 offset 一直保持不变，就会一直拉取一样的消息
        consumer.updateConsumeOffset(mq, offset);
        List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
        for (MessageExt msg : msgFoundList) {
          byte[] body = msg.getBody();
          System.out.println("consumeMessage(): " + new String(body) + " msgId: " + msg.getMsgId()
              + " queueId: " + msg.getQueueId() + " queueOffset: " + msg.getQueueOffset());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      Thread.sleep(1 * 10 * 1000);
    }
    //consumer.shutdown();
  }
}
