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

package com.ning.client;

import java.nio.ByteBuffer;

/**
 * Callback for the result of a single RPC. This will be
 * invoked once with either success or
 * failure.
 * RPC的回调。RPC成功或者失败都会回调。
 * 针对向服务端发送请求的回调  例如client向server发送消息
 * 等待server响应，server处理完消息，响应客户端的回调调用onSuccess或者失败响应调用onFailure
 */
public interface RpcResponseCallback {
  /**
   * Successful serialized result from server.
   *
   * After `onSuccess` returns, `response` will
   * be recycled and its content will become invalid.
   * Please copy the content of `response` if you want
   * to use it after `onSuccess` returns.
   *服务器成功序列化的结果
   *在onSuccess返回后，reponse的内容会被回收，同时response的内容将会变得无效
   * 如果你想要在onSuccess返回后使用response的内容，请拷贝response的内容
   */
  void onSuccess(ByteBuffer response);

  /** Exception either propagated from server or raised on client side.
    服务端传播的错误或者客户端发生错误回调该方法
   */
  void onFailure(Throwable e);
}
