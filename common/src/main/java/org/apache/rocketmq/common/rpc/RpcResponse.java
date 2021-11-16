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
package org.apache.rocketmq.common.rpc;

public class RpcResponse   {
    private RpcHeader header;
    private Object body;
    public RpcException exception;

    public RpcResponse() {

    }

    public RpcResponse(RpcHeader header, byte[] body) {
        this.header = header;
        this.body = body;
    }

    public RpcResponse(RpcException rpcException) {
        this.header = new RpcHeader(rpcException.getErrorCode());
        this.exception = rpcException;
    }

    public RpcHeader getHeader() {
        return header;
    }

    public void setHeader(RpcHeader header) {
        this.header = header;
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    public RpcException getException() {
        return exception;
    }

    public void setException(RpcException exception) {
        this.exception = exception;
    }

}