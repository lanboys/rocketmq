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
package org.apache.rocketmq.example.oopcoder.topic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.UUID;

public class AutoCreateTopicTest {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("p_group");
        // 如果是自定义模板主题，autoCreateTopicEnable = false 也可以，但是
        // 需要在控制台设置将 aaaaa 的权限设置为 7，意思是需要读写权限和继承权限
        // producer.setCreateTopicKey("aaaaa");
        producer.setNamesrvAddr("localhost:19876");
        producer.start();

        int index = 0;
        while (true) {
            int read = System.in.read();
            if (read == 10) { // 回车
                continue;
            }

            String pre = UUID.randomUUID().toString().substring(33);

            int all = 1;
            String time = UtilAll.timeMillisToHumanString2(System.currentTimeMillis());
            for (int i = index; i < index + all; i++) {
                try {
                    byte[] bytes = ("消息-" + i + "-" + time).getBytes(RemotingHelper.DEFAULT_CHARSET);

                    // 每次主题不一致
                    String topic = "topic-" + pre + "-" + i;

                    Message msg = new Message(topic, "Tag-index-" + i, "key-" + time + "-" + i, bytes);
                    // 通常没抛异常表示发送消息成功了，只是状态有多种
                    System.out.println("====================================================");
                    System.out.printf("msg: %s%n", msg);
                    System.out.println("-----------");
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("sendResult: %s%n", sendResult);
                    System.out.println("====================================================");
                    Thread.sleep(10);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            index += all;
        }
    }
}
