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

import com.ning.buffer.ManagedBuffer;
import com.ning.client.TransportClient;
import io.netty.channel.Channel;

/**
 * The StreamManager is used to fetch individual chunks from a stream. This is used in
 * {@link TransportRequestHandler} in order to respond to fetchChunk() requests. Creation of the
 * stream is outside the scope of the transport layer, but a given stream is guaranteed to be read
 * by only one client connection, meaning that getChunk() for a particular stream will be called
 * serially and that once the connection associated with the stream is closed, that stream will
 * never be used again.
 *这个StreamManager是用来获取流中的单个块。
 *TransportRequestHandler使用这个StreamManager以便来响应客户端的fetchChunk()请求方法。
 *StreamManager的创建是在传输层外面的，但是一个给定的流是保证只能被一个客户端读取，意味着针对特定流的getChunk方法
 * 将会被顺序的调用，然后一旦和这个流的相关连接关闭，流将不能继续被使用。
 *
 *
 */
public abstract class StreamManager {
  /**
   * Called in response to a fetchChunk() request. The returned buffer will be passed as-is to the
   * client. A single stream will be associated with a single TCP connection, so this method
   * will not be called in parallel for a particular stream.
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   *
   * The returned ManagedBuffer will be release()'d after being written to the network.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
    响应客户端的fetchChunk方法时调用，返回的缓冲内容将会原样的传递给客户端。单个流将会和单个TCP连接相关联，因此特定的流不会并行的
    调用这个方法。
    可以按任何顺序请求块，也可以重复请求块，但是没有要求一定支持这种行为。
    返回的ManagedBuffer将会release在写出网络后。

   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * Called in response to a stream() request. The returned data is streamed to the client
   * through a single TCP connection.
   *
   * Note the <code>streamId</code> argument is not related to the similarly named argument in the
   * {@link #getChunk(long, int)} method.
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.

    响应流请求，将通过单一的TCP连接返回给客户端

   */
  public ManagedBuffer openStream(String streamId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Associates a stream with a single client connection, which is guaranteed to be the only reader
   * of the stream. The getChunk() method will be called serially on this connection and once the
   * connection is closed, the stream will never be used again, enabling cleanup.
   *
   * This must be called before the first getChunk() on the stream, but it may be invoked multiple
   * times with the same channel and stream id.
   *
   *将与单一的客户端相关联，这被保证是流的唯一读取者。
   *这个方法必须在getChunk方法之前调用，但是可以使用相同的通道和流ID调用多次
   *
   */
  public void registerChannel(Channel channel, long streamId) { }

  /**
   * Indicates that the given channel has been terminated. After this occurs, we are guaranteed not
   * to read from the associated streams again, so any state can be cleaned up.
   *
   *表示给定的通道已经被终止，在这个发生后，我们就不保证能再次从相关的流读取数据，所以可以清楚任何状态
   */
  public void connectionTerminated(Channel channel) { }

  /**
   * Verify that the client is authorized to read from the given stream.
   *
   * @throws SecurityException If client is not authorized.
   *
   *验证客户端是否有权从给定的流读取数据
   */
  public void checkAuthorization(TransportClient client, long streamId) { }

}
