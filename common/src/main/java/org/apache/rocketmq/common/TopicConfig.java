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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.constant.PermName;

/**
 * 读写队列
 *
 * 从物理上来讲，读写队列是同一个队列。所以，不存在读写队列数据同步问题。读写队列是逻辑上进行区
 * 分的概念。一般情况下，读写队列数量是一样的。
 *
 * 例如,创建Topic时设置的写队列数量为8,读队列数量为4,此时系统会创建8个 Queue,分别是0 1 2 3 4 5 6 7。
 * Producer会将消息写入到这8个队列,但 Consumer只会消费0 1 2 3这4个队列中的消息,4 5 6 7中的消息是不会被消费到的。
 *
 * 再如,创建 Topic 时设置的写队列数量为4,读队列数量为8,此时系统会创建8个 Queue,分别是0 1 2 3 4 5 6 7。
 * Producer会将消息写入到0 1 2 3这4个队列,但 Consumer只会消费0 1 2 3 4 5 6 7这8个队列中的消息,但是4567中
 * 是没有消息的。此时假设Consumer Group中包含两个Consumer，Consumer1消费0 1 2 3，而Consumer2消费4 5 6 7。
 * 但实际情况，Consumer2是没有消息可以消费。
 *
 * 也就是当读写队列数量不一致时，总是有问题的。其这样设计的目的是为了方便Topic的Queue的缩容。
 *
 * 例如,原来创建的Topic中包含16个Queue,如何能够使其Queue缩容为8个,还不会丢失消息?可以动态修改写队列数量
 * 为8,读队列数量不变。此时新的消息只能写入到前8个队列,而消费都消费的却是16个队列中的数据。当发现后8个
 * Queue中的消息消费完毕后,就可以再将读队列数量动态设置为8.
 *
 * perm
 * perm用于设置对当前创建Topic的操作权限：2表示只写，4表示只读，6表示读写
 */
public class TopicConfig {
    private static final String SEPARATOR = " ";
    public static int defaultReadQueueNums = 16;
    public static int defaultWriteQueueNums = 16;
    private String topicName;
    private int readQueueNums = defaultReadQueueNums;
    private int writeQueueNums = defaultWriteQueueNums;
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;
    private int topicSysFlag = 0;
    private boolean order = false;

    public TopicConfig() {
    }

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.topicName);
        sb.append(SEPARATOR);
        sb.append(this.readQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.writeQueueNums);
        sb.append(SEPARATOR);
        sb.append(this.perm);
        sb.append(SEPARATOR);
        sb.append(this.topicFilterType);

        return sb.toString();
    }

    public boolean decode(final String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs != null && strs.length == 5) {
            this.topicName = strs[0];

            this.readQueueNums = Integer.parseInt(strs[1]);

            this.writeQueueNums = Integer.parseInt(strs[2]);

            this.perm = Integer.parseInt(strs[3]);

            this.topicFilterType = TopicFilterType.valueOf(strs[4]);

            return true;
        }

        return false;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public TopicFilterType getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(TopicFilterType topicFilterType) {
        this.topicFilterType = topicFilterType;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean isOrder) {
        this.order = isOrder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final TopicConfig that = (TopicConfig) o;

        if (readQueueNums != that.readQueueNums)
            return false;
        if (writeQueueNums != that.writeQueueNums)
            return false;
        if (perm != that.perm)
            return false;
        if (topicSysFlag != that.topicSysFlag)
            return false;
        if (order != that.order)
            return false;
        if (topicName != null ? !topicName.equals(that.topicName) : that.topicName != null)
            return false;
        return topicFilterType == that.topicFilterType;

    }

    @Override
    public int hashCode() {
        int result = topicName != null ? topicName.hashCode() : 0;
        result = 31 * result + readQueueNums;
        result = 31 * result + writeQueueNums;
        result = 31 * result + perm;
        result = 31 * result + (topicFilterType != null ? topicFilterType.hashCode() : 0);
        result = 31 * result + topicSysFlag;
        result = 31 * result + (order ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TopicConfig [topicName=" + topicName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + PermName.perm2String(perm)
            + ", topicFilterType=" + topicFilterType + ", topicSysFlag=" + topicSysFlag + ", order="
            + order + "]";
    }
}
