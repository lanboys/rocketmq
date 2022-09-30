package org.apache.rocketmq.example.oopcoder.start;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.example.utils.SendMessageUtil;

public class Producer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("p-group");
        producer.setNamesrvAddr("localhost:19876");
        producer.start();

        SendMessageUtil.sendByConsole(producer);
    }
}
