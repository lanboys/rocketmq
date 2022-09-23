// package org.apache.rocketmq.example.oopcoder.transaction;
//
// import org.apache.rocketmq.client.exception.MQClientException;
// import org.apache.rocketmq.client.producer.LocalTransactionState;
// import org.apache.rocketmq.client.producer.TransactionListener;
// import org.apache.rocketmq.client.producer.TransactionMQProducer;
// import org.apache.rocketmq.common.message.Message;
// import org.apache.rocketmq.common.message.MessageExt;
// import org.apache.rocketmq.example.utils.PrintUtil;
// import org.apache.rocketmq.example.utils.SendMessageUtil;
//
// import java.io.IOException;
// import java.util.concurrent.ArrayBlockingQueue;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.ThreadFactory;
// import java.util.concurrent.ThreadPoolExecutor;
// import java.util.concurrent.TimeUnit;
// import java.util.concurrent.atomic.AtomicInteger;
//
// public class OuterTransactionProducer {
//
//     private static final AtomicInteger transactionIndex = new AtomicInteger(0);
//     // 模拟数据库
//     private static final ConcurrentHashMap<String, Message> localTrans = new ConcurrentHashMap<>();
//
//     public static void main(String[] args) throws MQClientException, InterruptedException, IOException {
//         // 消息在事务外部发送
//         TransactionMQProducer producer = getTransactionMQProducer(new OuterTransactionListener());
//
//         SendMessageUtil.sendInTransaction(producer);
//     }
//
//     public static TransactionMQProducer getTransactionMQProducer(TransactionListener transactionListener) {
//         try {
//             TransactionMQProducer producer = new TransactionMQProducer("tx-group");
//             ExecutorService executorService = new ThreadPoolExecutor(2, 5,
//                     100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000),
//                     new ThreadFactory() {
//                         private AtomicInteger threadIdx = new AtomicInteger(0);
//
//                         @Override
//                         public Thread newThread(Runnable r) {
//                             Thread thread = new Thread(r);
//                             thread.setName("client-transaction-msg-check-thread-" + threadIdx.getAndIncrement());
//                             return thread;
//                         }
//                     });
//             producer.setExecutorService(executorService);
//             producer.setNamesrvAddr("localhost:19876");
//             producer.setTransactionListener(transactionListener);
//             producer.start();
//             return producer;
//         } catch (MQClientException e) {
//             e.printStackTrace();
//         }
//         return null;
//     }
//
//     // 通常这里是一个数据库事务
//     // @Transactional(rollbackFor = Exception.class)
//     public static String outerService(Message msg, Object arg) {
//         try {
//             // 模拟业务耗时操作
//             TimeUnit.SECONDS.sleep(1);
//         } catch (InterruptedException e) {
//             //    ignore
//         }
//         int value = transactionIndex.getAndIncrement();
//         int status = value % 2;
//         if (status == 0) {
//             throw new RuntimeException("数据库事务回滚了... " + msg.getTransactionId());
//         }
//         // 模拟保存到事务表中，或者保存到业务表中的某个字段里，表明本地事务提交成功了
//         localTrans.put(msg.getTransactionId(), msg);
//         return "ok";
//     }
//
//     public static class OuterTransactionListener implements TransactionListener {
//
//         @Override
//         public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
//             System.out.println("事务半消息发送成功，开始执行本地事务... " + msg.getTransactionId());
//             PrintUtil.printMessage(msg);
//             try {
//                 // 这个结果无法返回给外部调用的方法
//                 String result = outerService(msg, arg);
//                 return LocalTransactionState.COMMIT_MESSAGE;
//                 // return LocalTransactionState.UNKNOW;
//             } catch (Throwable e) {
//                 // 事务回滚了
//                 e.printStackTrace();
//             }
//
//             // 这种方法通常是明确知道，事务消息到底是需要提交还是回滚，不存在未知状态，所以事务回查也不会执行了
//             // 但是有时候为了安全起见，也可以在事务执行成功后直接返回未知状态，然后再通过事务回查方法来确定事务是否已经提交
//             return LocalTransactionState.ROLLBACK_MESSAGE;
//         }
//
//         @Override
//         public LocalTransactionState checkLocalTransaction(MessageExt msg) {
//             // checkLocalTransaction 执行本地事务的时候，其实已经知道事务到底是提交还是回滚，不存在未知状态，所以事务回查也不会执行了
//             System.out.println("开始执行本地事务回查方法... " + msg.getTransactionId());
//             PrintUtil.printMessage(msg);
//
//             // 模拟从事务表或者业务表中查询是否存在 msgId，存在意味着本地事务已经提交成功，
//             // 那么事务半消息就可以正式提交到对应的 topic 队列里面了
//             Message mysqlMessage = localTrans.get(msg.getTransactionId());
//             if (mysqlMessage != null) {
//                 System.out.println("本地事务回查成功，提交消息... " + msg.getTransactionId());
//                 return LocalTransactionState.COMMIT_MESSAGE;
//             }
//             System.out.println("本地事务回查失败，回滚消息... " + msg.getTransactionId());
//             return LocalTransactionState.ROLLBACK_MESSAGE;
//         }
//     }
// }
