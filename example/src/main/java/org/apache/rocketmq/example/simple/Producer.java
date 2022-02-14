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

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.UUID;

public class Producer {

  public static void main(String[] args) throws Exception {

    DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
    // 更新路由信息定时任务时间间隔
    producer.setPollNameServerInterval(1000 * 18000);
    // 故障延迟机制
    producer.setSendLatencyFaultEnable(true);

    producer.setNamesrvAddr("localhost:9876");
    producer.start();

    int index = 0;
    while (true) {
      int read = System.in.read();
      if (read == 10) { // 回车
        continue;
      }
      int all = 1;
      long l = System.currentTimeMillis();
      for (int i = index; i < index + all; i++) {
        try {
          byte[] bytes = ("我是消息-" + i + "-" + UUID.randomUUID().toString()).getBytes(RemotingHelper.DEFAULT_CHARSET);
          Message msg = new Message("aaaaa", "Tag-index-" + i, "key-" + l + "-" + i, bytes);
          SendResult sendResult = producer.send(msg);
          System.out.printf("%s%n", sendResult);
          Thread.sleep(10);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      index += all;
    }
  }
}
