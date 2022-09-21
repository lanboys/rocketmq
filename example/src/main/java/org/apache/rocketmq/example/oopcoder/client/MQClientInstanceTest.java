package org.apache.rocketmq.example.oopcoder.client;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Created by lanbing at 2022/9/21 10:58
 */

public class MQClientInstanceTest {

    public static void main(String[] args) throws Exception {
        test();
        // test1();
        // test2();
    }

    // 启动两个进程，同一个 group 多个消费者，都分布在不同的jvm进程中，控制台页面 最终会出现 clientId ( 172.27.26.90@instanceName ) 一样的实例
    private static void test() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("p-group");
        // 设置实例名称, 默认是进程 id, ClientConfig.buildMQClientId()
        // System.setProperty("rocketmq.client.name", "instanceName");
        producer.setInstanceName("instanceName");
        producer.setNamesrvAddr("localhost:19876");
        producer.start();
        sendMessage(producer);// 为了在控制台 Producer 页面中搜到，不发消息可能没那么快注册到 namesrv 中
    }

    // 一个 jvm 进程中，同一个 group 想要多个消费者实例，那就只能创建多个 MQClientInstance 实例了
    // 实际使用中，应该比较少出现，通常微服务架构中，同一个 group 多个消费者，都分布在不同的服务(jvm进程)中
    private static void test1() throws MQClientException {
        int pid = UtilAll.getPid();

        DefaultMQProducer producer = new DefaultMQProducer("p-group");
        producer.setInstanceName("instanceName-1-" + pid);
        producer.setNamesrvAddr("localhost:19876");
        producer.start();
        sendMessage(producer);

        DefaultMQProducer producer1 = new DefaultMQProducer("p-group");
        producer1.setInstanceName("instanceName-2-" + pid);
        producer1.setNamesrvAddr("localhost:19876");
        producer1.start();
        sendMessage(producer1);
    }

    // 客户端配置，由第一个启动的 生产者/消费者/admin实例 来决定
    private static void test2() throws MQClientException {
        int pid = UtilAll.getPid();

        DefaultMQProducer producer = new DefaultMQProducer("p-group-1");
        producer.setInstanceName("instanceName-" + pid);
        producer.setNamesrvAddr("localhost:19876");
        producer.start();
        sendMessage(producer);

        DefaultMQProducer producer1 = new DefaultMQProducer("p-group-2");
        producer1.setInstanceName("instanceName-" + pid);
        // 客户端配置，由第一个启动的 生产者/消费者/admin实例 来决定
        producer1.setNamesrvAddr("localhost:1234");// 注意端口是不对的
        producer1.start();
        sendMessage(producer1);
    }

    private static void sendMessage(DefaultMQProducer producer) {
        try {
            Message msg = new Message("aaaaa",
                    ("消息-" + UtilAll.timeMillisToHumanString2(System.currentTimeMillis())).getBytes(RemotingHelper.DEFAULT_CHARSET));

            System.out.println("====================================================");
            System.out.printf("消息内容: %s%n", msg);
            System.out.println("----------------------------");
            SendResult sendResult = producer.send(msg);
            System.out.printf("发送结果: %s%n", sendResult);
            System.out.println("====================================================");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
