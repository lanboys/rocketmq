package org.apache.rocketmq.example.oopcoder.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.example.utils.PrintUtil.simpleDatePrintf;
import static org.apache.rocketmq.example.utils.PrintUtil.simpleDatePrintln;

/*
 * executeLocalTransaction 方法在事务内部执行
 */
public class InnerTransactionProducer {

    // 消息在事务内部发送
    private static TransactionMQProducer producer;
    private static final AtomicInteger transactionIndex = new AtomicInteger(0);
    // 模拟数据库
    private static final ConcurrentHashMap<String, String> localTrans = new ConcurrentHashMap<>();

    public static void main(String[] args) throws MQClientException, InterruptedException, IOException {
        producer = getTransactionMQProducer(new InnerTransactionListener());

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
            try {
                outerService();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // 通常这里是一个数据库事务
    // @Transactional(rollbackFor = Exception.class)
    public static String outerService() {
        SendResult sendResult = sendMessageInTransaction();
        if (sendResult == null) {
            throw new RuntimeException("消息发送异常，整个事务需要回滚..");
        }
        // broker 记得用单机配置文件启动 xxx\.run\conf\standalone-broker.conf，不然报错，slave不可用 SLAVE_NOT_AVAILABLE
        // 其他配置文件也可以，不过需要启动集群，比较复杂
        if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
            throw new RuntimeException("消息发送失败了，整个事务需要回滚.. " + sendResult.getMsgId());
        }

        // 模拟保存到事务表中，或者保存到业务表中的某个字段里，表明本地事务提交成功了
        localTrans.put(sendResult.getMsgId(), "ok");
        simpleDatePrintln("插入事务消息表成功... " + sendResult.getMsgId());

        try {
            // 模拟业务耗时操作
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            // ignore
        }

        int value = transactionIndex.getAndIncrement();
        int status = value % 2;
        if (status == 0) {
            // 模拟数据库回滚
            localTrans.remove(sendResult.getMsgId());
            throw new RuntimeException("数据库事务回滚了.. " + sendResult.getMsgId());
        }

        return "ok";
    }

    public static SendResult sendMessageInTransaction() {
        String time = UtilAll.timeMillisToHumanString2(System.currentTimeMillis());
        String tag = UUID.randomUUID().toString().substring(33);
        try {
            byte[] bytes = ("事务消息-" + time).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message msg = new Message("aaaaa", "tag-" + tag, "key-" + time, bytes);
            simpleDatePrintln("====================================================");
            simpleDatePrintln("事务消息...");
            simpleDatePrintf("消息内容: %s%n", msg);
            simpleDatePrintln("----------------------------");
            SendResult sendResult = producer.sendMessageInTransaction(msg, null);
            simpleDatePrintf("发送结果: %s%n", sendResult);
            simpleDatePrintln("====================================================");
            return sendResult;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class InnerTransactionListener implements TransactionListener {

        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
            simpleDatePrintln("事务半消息发送成功，本地事务早已开始执行，但是未结束，本地事务状态未知... " + msg.getTransactionId());

            // 这里的异常会被吃掉，导致事务无法回滚？？？ 如果业务逻辑在外面执行，插入事务消息表操作不能这里做，除非这里内部就是一个单独事务

            // 事务未结束，无法知道本地事务是提交还是回滚，只能等待事务回查来确定

            // 大量的事务检查会导致系统性能受损，容易导致事务处理延迟。这是个问题
            return LocalTransactionState.UNKNOW;
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            simpleDatePrintln("开始执行本地事务回查方法... " + msg.getTransactionId());
            // PrintUtil.printMessage(msg);

            // 模拟从事务表或者业务表中查询是否存在 msgId，存在意味着本地事务已经提交成功，
            // 那么事务半消息就可以正式提交到对应的 topic 队列里面了
            // 这里注意要用 transactionId
            String mysqlMessage = localTrans.get(msg.getTransactionId());
            if (mysqlMessage != null) {
                simpleDatePrintln("本地事务回查成功，提交消息... " + msg.getTransactionId());
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            simpleDatePrintln("本地事务回查失败，回滚消息... " + msg.getTransactionId());
            return LocalTransactionState.ROLLBACK_MESSAGE;

            // ======================
            // 以下是为了测试
            // long status = System.currentTimeMillis() % 2;
            // if (status == 0) {
            //     logPrint("本地事务回查失败，回滚消息... " + msg.getTransactionId());
            //     return LocalTransactionState.ROLLBACK_MESSAGE;
            // }
            //
            // // 模拟未知状态，如果本地事务长时间未结束，回查就可能出现未知状态
            // logPrint("本地事务回查状态未知... " + msg.getTransactionId());
            // return LocalTransactionState.UNKNOW;
        }
    }

    private static TransactionMQProducer getTransactionMQProducer(TransactionListener transactionListener) {
        try {
            TransactionMQProducer producer = new TransactionMQProducer("tx-group");
            ExecutorService executorService = new ThreadPoolExecutor(2, 5,
                    100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000),
                    new ThreadFactory() {
                        private AtomicInteger threadIdx = new AtomicInteger(0);

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread thread = new Thread(r);
                            thread.setName("client-transaction-msg-check-thread-" + threadIdx.getAndIncrement());
                            return thread;
                        }
                    });
            producer.setExecutorService(executorService);
            producer.setNamesrvAddr("localhost:19876");
            producer.setTransactionListener(transactionListener);
            producer.start();
            return producer;
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        return null;
    }
}
