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
package org.apache.rocketmq.example.oopcoder.viewMessage;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.example.utils.PrintUtil;

public class ViewMessage {

    public static void main(String[] args) throws Exception {
        // DefaultMQProducer producer = new DefaultMQProducer("p_group");
        // producer.setNamesrvAddr("localhost:19876");
        // producer.start();

        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("c-group");
        consumer.setNamesrvAddr("localhost:19876");
        consumer.start();
        System.out.printf("Consumer Started.%n");
        // MessageId 就是 uniqKey
        MessageExt message = consumer.viewMessage("RMQ_SYS_TRANS_HALF_TOPIC", "AC1B1A5A48B418B4AAC26E13480B0000");
        // MessageExt message = consumer.viewMessage("%RETRY%c-group", "AC1B1A5A385C18B4AAC2606CE0F80000");
        // MessageExt message = consumer.viewMessage("aaaaa", "AC1B1A5A385C18B4AAC2606CE0F80000");
        PrintUtil.printMessage(message);

        consumer.shutdown();
    }
}
