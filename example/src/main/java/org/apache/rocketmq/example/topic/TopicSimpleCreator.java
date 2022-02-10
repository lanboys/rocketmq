package org.apache.rocketmq.example.topic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

public class TopicSimpleCreator {

  public static void main(String[] args) {
    DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
    producer.setNamesrvAddr("localhost:9876");
    try {
      producer.start();

      // key的作用，获取broker address
      String key = "cluster-1";
      producer.createTopic(key, "aaaaa", 4);
      System.out.println("topic were created .");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.shutdown();
    }
  }
}
