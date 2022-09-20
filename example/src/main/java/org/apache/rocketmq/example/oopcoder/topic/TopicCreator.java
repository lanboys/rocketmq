package org.apache.rocketmq.example.oopcoder.topic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

import java.util.UUID;

public class TopicCreator {

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("p_group");
        producer.setNamesrvAddr("localhost:19876");
        try {
            producer.start();
            String pre = UUID.randomUUID().toString().substring(33);
            for (int i = 0; i < 10; i++) {

                // Topic创建的时候可以用集群模式去创建（这样集群里面每个broker的queue的数量相同），
                // 也可以用单个broker模式去创建（这样每个broker的queue数量可以不一致）

                // key的作用，获取broker address
                String key = "cluster-1";
                producer.createTopic(key, "topic-" + pre + "-" + i, 1);
            }
            System.out.println("topic were created .");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
        }
    }
}
