package org.apache.rocketmq.broker.plugin;

import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;

/**
 * 自定义消息存储引擎
 */

public class MyAbstractPluginMessageStore extends AbstractPluginMessageStore {

  public MyAbstractPluginMessageStore(MessageStorePluginContext context, MessageStore next) {
    super(context, next);
  }

  @Override
  public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
    System.out.println("putMessages() begin");
    PutMessageResult putMessageResult = next.putMessages(messageExtBatch);
    System.out.println("putMessages() end");
    return putMessageResult;
  }

  @Override
  public PutMessageResult putMessage(MessageExtBrokerInner msg) {
    System.out.println("putMessages() begin");
    PutMessageResult putMessageResult = super.putMessage(msg);
    System.out.println("putMessages() end");
    return putMessageResult;
  }
}
