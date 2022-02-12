/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TransactionProducer {

  public static void main(String[] args) throws MQClientException, InterruptedException, IOException {
    TransactionListener transactionListener = new TransactionListenerImpl();
    TransactionMQProducer producer = new TransactionMQProducer("p-group");
    ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("client-transaction-msg-check-thread");
        return thread;
      }
    });

    producer.setExecutorService(executorService);
    producer.setTransactionListener(new TransactionListener() {
      private AtomicInteger transactionIndex = new AtomicInteger(0);

      private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

      @Override
      public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        int value = transactionIndex.getAndIncrement();
        int status = value % 3;
        localTrans.put(msg.getTransactionId(), status);
        return LocalTransactionState.UNKNOW;
        //return LocalTransactionState.COMMIT_MESSAGE;
        //return LocalTransactionState.ROLLBACK_MESSAGE;
      }

      @Override
      public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        Integer status = localTrans.get(msg.getTransactionId());
        return LocalTransactionState.UNKNOW;
        //return LocalTransactionState.COMMIT_MESSAGE;
        //return LocalTransactionState.ROLLBACK_MESSAGE;
      }
    });
    producer.setNamesrvAddr("localhost:9876");
    producer.start();
    int index = 0;
    while (true) {
      int read = System.in.read();
      if (read == 10) { // 回车
        continue;
      }
      String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
      int all = 1;
      for (int i = index; i < index + all; i++) {
        try {
          Message msg = new Message("aaaaa", tags[i % tags.length], "消息键-KEY-" + i, ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
          SendResult sendResult = producer.sendMessageInTransaction(msg, null);
          System.out.printf("%s%n", sendResult);

          Thread.sleep(10);
        } catch (MQClientException | UnsupportedEncodingException e) {
          e.printStackTrace();
        }
      }
      index += all;
    }
  }
}
