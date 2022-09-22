package org.apache.rocketmq.example.utils;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Date;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX;

/**
 * Created by lanbing at 2022/9/19 17:46
 */

public class PrintUtil {

    public static void printMessage(MessageExt msg) {
        System.out.println("--------------------------------------------");
        System.out.println("==> time: " + UtilAll.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss:SSS"));
        System.out.println("==> thread: " + Thread.currentThread().getName());
        System.out.println("==> msgId: " + msg.getMsgId());
        System.out.println("==> transactionId: " + msg.getTransactionId());
        System.out.println("==> tags: " + msg.getTags());
        System.out.println("==> keys: " + msg.getKeys());
        System.out.println("==> topic: " + msg.getTopic());
        System.out.println("==> queueId: " + msg.getQueueId());
        System.out.println("==> queueOffset: " + msg.getQueueOffset());
        System.out.println("==> commitLogOffset: " + msg.getCommitLogOffset());
        System.out.println("==> reconsumeTimes: " + msg.getReconsumeTimes());
        System.out.println("==> bornTime: " + UtilAll.formatDate(new Date(msg.getBornTimestamp()), "yyyy-MM-dd HH:mm:ss:SSS"));
        System.out.println("==> storeTime: " + UtilAll.formatDate(new Date(msg.getStoreTimestamp()), "yyyy-MM-dd HH:mm:ss:SSS"));
        System.out.println("==> body: " + new String(msg.getBody()));
        System.out.println("--------------------------------------------");
    }

    public static void printMessage(Message msg) {
        System.out.println("--------------------------------------------");
        System.out.println("==> time: " + UtilAll.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss:SSS"));
        System.out.println("==> thread: " + Thread.currentThread().getName());
        System.out.println("==> msgId: " + msg.getProperty(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        System.out.println("==> transactionId: " + msg.getTransactionId());
        System.out.println("==> tags: " + msg.getTags());
        System.out.println("==> keys: " + msg.getKeys());
        System.out.println("==> topic: " + msg.getTopic());
        // System.out.println("==> queueId: " + msg.getQueueId());
        // System.out.println("==> queueOffset: " + msg.getQueueOffset());
        // System.out.println("==> commitLogOffset: " + msg.getCommitLogOffset());
        // System.out.println("==> reconsumeTimes: " + msg.getReconsumeTimes());
        // System.out.println("==> bornTime: " + UtilAll.formatDate(new Date(msg.getBornTimestamp()), "yyyy-MM-dd HH:mm:ss:SSS"));
        // System.out.println("==> storeTime: " + UtilAll.formatDate(new Date(msg.getStoreTimestamp()), "yyyy-MM-dd HH:mm:ss:SSS"));
        System.out.println("==> body: " + new String(msg.getBody()));
        System.out.println("--------------------------------------------");
    }

}
