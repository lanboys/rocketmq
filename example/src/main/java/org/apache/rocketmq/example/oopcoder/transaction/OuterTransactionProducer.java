package org.apache.rocketmq.example.oopcoder.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.example.utils.PrintUtil;
import org.apache.rocketmq.example.utils.SendMessageUtil;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.example.utils.PrintUtil.simpleDatePrintln;

/*
 * executeLocalTransaction 方法 包裹着整个事务，也是官方案例中的用法
 */
public class OuterTransactionProducer {

    private static final AtomicInteger transactionIndex = new AtomicInteger(0);
    // 模拟数据库
    private static final ConcurrentHashMap<String, Message> localTrans = new ConcurrentHashMap<>();

    public static void main(String[] args) throws MQClientException, InterruptedException, IOException {
        // 消息在事务外部发送
        TransactionMQProducer producer = getTransactionMQProducer(new OuterTransactionListener());

        SendMessageUtil.sendInTransaction(producer);
    }

    public static TransactionMQProducer getTransactionMQProducer(TransactionListener transactionListener) {
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

    // 通常这里是一个数据库事务
    // @Transactional(rollbackFor = Exception.class)
    public static String outerService(Message msg, Object arg) {
        try {
            // 模拟业务耗时操作
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            //    ignore
        }
        int value = transactionIndex.getAndIncrement();
        int status = value % 2;
        if (status == 0) {
            throw new RuntimeException("数据库事务回滚了... " + msg.getTransactionId());
        }
        // 模拟保存到事务表中，或者保存到业务表中的某个字段里，表明本地事务提交成功了
        localTrans.put(msg.getTransactionId(), msg);
        return "ok";
    }

    public static class OuterTransactionListener implements TransactionListener {

        // https://rocketmq.apache.org/zh/docs/4.x/producer/06message5

        // 事务消息发送步骤如下：
        //
        // 1.生产者将半事务消息发送至  Broker。
        // 2.RocketMQ Broker 将消息持久化成功之后，向生产者返回 Ack 确认消息已经发送成功，此时消息暂不能投递，为半事务消息。

        // 3.生产者开始执行本地事务逻辑。

        // 4.生产者根据本地事务执行结果向服务端提交二次确认结果（Commit或是Rollback），服务端收到确认结果后处理逻辑如下：
        //     二次确认结果为Commit：服务端将半事务消息标记为可投递，并投递给消费者。
        //     二次确认结果为Rollback：服务端将回滚事务，不会将半事务消息投递给消费者。
        // 5.在断网或者是生产者应用重启的特殊情况下，若服务端未收到发送者提交的二次确认结果，
        //   或服务端收到的二次确认结果为Unknown未知状态，经过固定时间后，服务端将对消息生产者即生产者集群中任一生产者实例发起消息回查。
        //
        // 6.:::note 需要注意的是，服务端仅仅会按照参数尝试指定次数，超过次数后事务会强制回滚，
        // 因此未决事务的回查时效性非常关键，需要按照业务的实际风险来设置 :::
        //
        // 事务消息回查步骤如下：
        //     7. 生产者收到消息回查后，需要检查对应消息的本地事务执行的最终结果。
        //     8. 生产者根据检查得到的本地事务的最终状态再次提交二次确认，服务端仍按照步骤4对半事务消息进行处理。

        // https://help.aliyun.com/zh/apsaramq-for-rocketmq/cloud-message-queue-rocketmq-5-x-series/developer-reference/transactional-messages
        // 避免大量未决事务导致超时
        // 云消息队列 RocketMQ 版支持在事务提交阶段异常的情况下发起事务回查，保证事务一致性。
        // 但生产者应该尽量避免本地事务返回未知结果。大量的事务检查会导致系统性能受损，容易导致事务处理延迟。
        //
        // 正确处理“进行中”的事务
        // 消息回查时，对于正在进行中的事务不要返回Rollback或Commit结果，应继续保持Unknown的状态。
        //
        // 一般出现消息回查时事务正在处理的原因为：事务执行较慢，消息回查太快。解决方案如下：
        //      将第一次事务回查时间设置较大一些，但可能导致依赖回查的事务提交延迟较大。
        //      程序能正确识别正在进行中的事务。

        // 有一个配置是控制消息多久后，才运行回查
        // 首先检查的事务消息的最短时间，一条消息只超过此时间间隔可以检查。
        // private long transactionTimeOut = 6 * 1000;

        @Override
        public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {

            // ===== 官方例子是在这里开始执行本地事务，发消息的代码不再事务代码中，但是实际用起来不太好，比如想象一下，
            // ===== 在写接口的时候，这里都没法携带返回值，还是我的用法比较合乎常理。

            simpleDatePrintln("事务半消息发送成功，开始执行本地事务... " + msg.getTransactionId());
            PrintUtil.printMessage(msg);
            try {
                // 这个结果无法返回给外部调用的方法
                String result = outerService(msg, arg);
                // 执行到这里表示 本地mysql事务已经成功提交
                return LocalTransactionState.COMMIT_MESSAGE;
            } catch (Throwable e) {
                // 事务回滚了
                e.printStackTrace();

                // 这种方法通常是明确知道，事务消息到底是需要提交还是回滚，不存在未知状态，所以事务回查也不会执行了
                // 但是有时候为了安全起见，也可以在事务执行失败后直接返回未知状态，然后再通过事务回查方法来确定事务是否已经提交
                return LocalTransactionState.UNKNOW;
            }
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt msg) {
            // checkLocalTransaction 执行本地事务的时候，其实已经知道事务到底是提交还是回滚，不存在未知状态，所以事务回查也不会执行了
            simpleDatePrintln("开始执行本地事务回查方法... " + msg.getTransactionId());
            PrintUtil.printMessage(msg);

            // 模拟从事务表或者业务表中查询是否存在 msgId，存在意味着本地事务已经提交成功，
            // 那么事务半消息就可以正式提交到对应的 topic 队列里面了
            Message mysqlMessage = localTrans.get(msg.getTransactionId());
            if (mysqlMessage != null) {
                simpleDatePrintln("本地事务回查成功，提交消息... " + msg.getTransactionId());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
            simpleDatePrintln("本地事务回查失败，回滚消息... " + msg.getTransactionId());
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }

}
