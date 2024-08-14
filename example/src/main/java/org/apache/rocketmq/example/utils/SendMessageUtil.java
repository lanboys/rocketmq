package org.apache.rocketmq.example.utils;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.IOException;
import java.util.*;

import static org.apache.rocketmq.example.utils.PrintUtil.simpleDatePrintf;
import static org.apache.rocketmq.example.utils.PrintUtil.simpleDatePrintln;

/**
 * Created by lanbing at 2022/9/21 15:46
 */

public class SendMessageUtil {

    public static void sendInTransaction(DefaultMQProducer producer) {
        doSendByConsole(producer, true);
    }

    public static void sendByConsole(DefaultMQProducer producer) {
        doSendByConsole(producer, false);
    }

    private static void doSendByConsole(DefaultMQProducer producer, boolean sendInTransaction) {
        int index = 0;
        while (true) {
            int read = 0;
            try {
                read = System.in.read();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (read == 10) { // 回车
                continue;
            }
            int all = 1;
            String time = UtilAll.timeMillisToHumanString2(System.currentTimeMillis());
            String tag = UUID.randomUUID().toString().substring(33);
            for (int i = index; i < index + all; i++) {
                try {
                    byte[] bytes = ("消息-" + i + "-" + time).getBytes(RemotingHelper.DEFAULT_CHARSET);
                    Message msg = new Message("aaaaa", "tag-" + tag + "-" + i, "key-" + time, bytes);
                    // 通常没抛异常表示发送消息成功了，只是状态有多种
                    simpleDatePrintln("====================================================");
                    if (sendInTransaction) {
                        simpleDatePrintln("事务消息...");
                    }
                    simpleDatePrintf("消息内容: %s%n", msg);
                    simpleDatePrintln("----------------------------");
                    SendResult sendResult = sendInTransaction ? producer.sendMessageInTransaction(msg, null) :
                            producer.send(msg);
                    simpleDatePrintf("发送结果: %s%n", sendResult);
                    simpleDatePrintln("====================================================");
                    Thread.sleep(10);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            index += all;
        }
    }
}
