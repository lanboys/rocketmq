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
            // String key = "standalone-cluster";
            // String key = "broker-1";

            for (String key : keys) {
                try {
                    producer.createTopic(key, "aaaaa", 4);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("topic were created .");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
        }
    }
}
