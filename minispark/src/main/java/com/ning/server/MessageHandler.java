/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ning.server;


import com.ning.protocol.Message;

/**
 * Handles either request or response messages coming off of Netty. A MessageHandler instance
 * is associated with a single Netty Channel (though it may have multiple clients on the same
 * Channel.)
 * 处理netty发送的请求或者响应消息。
 * 一个MessgeHandler和一个单一netty channel相关联。
 * 【尽管在管道上可能有多个客户端】
 *
 */
public abstract class MessageHandler<T extends Message> {
  /** Handles the receipt of a single message.
   *处理单个消息的接收
   * */
  public abstract void handle(T message) throws Exception;

  /** Invoked when an exception was caught on the Channel.
   *在channel捕获到异常的时候调用
   * */
  public abstract void exceptionCaught(Throwable cause);

  /** Invoked when the channel this MessageHandler is on has been unregistered.
   *当MessageHandler所在频道注销的时候调用
   * */
  public abstract void channelUnregistered();
}
