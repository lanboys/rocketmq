package org.apache.rocketmq.example.topic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class TopicSimpleCreator {

    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("localhost:19876");
        try {
            producer.start();

            // key 可以是 cluster 或 broker 的名称，这两者默认都是 系统topic，管理后台页面勾选 System，就可以看到
            // key 的作用，获取 broker 的地址
            // String key = "cluster-1";
            String key = "broker-1";
            producer.createTopic(key, "aaaaa", 4);
            System.out.println("topic were created .");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
        }
    }
}
