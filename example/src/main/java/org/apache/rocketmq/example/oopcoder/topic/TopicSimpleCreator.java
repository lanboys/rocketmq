package org.apache.rocketmq.example.oopcoder.topic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class TopicSimpleCreator {

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("p_group");
        producer.setNamesrvAddr("localhost:19876");
        try {
            producer.start();

            // key 可以是 cluster 或 broker 的名称，这两者默认都是 系统topic，管理后台页面勾选 System，就可以看到
            // key 的作用，获取 broker 的地址

            String[] keys = {"cluster-1", "cluster-2", "broker-1", "standalone-cluster", "standalone-broker"};
            // String[] keys = {"broker-1"};
            // String[] keys = {"standalone-cluster"};

            for (String key : keys) {
                try {
                    producer.createTopic(key, "aaaaa", 4);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("topic were created .");

            // topic一样，不同的broker，创建的队列数量不一样，是允许的，管理后台页面创建topic貌似有bug，达不到这种效果
            // producer.createTopic("broker-1", "aaaaa", 1);
            // producer.createTopic("broker-2", "aaaaa", 3);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
        }
    }
}
