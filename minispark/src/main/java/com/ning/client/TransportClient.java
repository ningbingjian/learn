package com.ning.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.ning.buffer.NioManagedBuffer;
import com.ning.protocol.*;
import com.ning.util.NettyUtils;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream.
 * This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of TransportClient using {@link //TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 *
 * Concurrency: thread safe and can be called from multiple threads.

 用于获取预先协商的流的连续块的客户端。该API旨在允许大数据分解成几百kb到几MB来进行高效的传输。
注意：虽然客户端处理从流【即数据层面】中抓取数据块，但是真正的流设置是在传输层之外进行设置的。
 客户端提供了便利的方法client.sendRpc来进行客户端和服务端之间的流交互来进行流的设置。。
 例如：一个经典的工作流可能如下：
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * client.sendRPC(new CloseStream(100))
 *
 * 可以使用TransportClientFactory来构造TransportClient实例，TransportClient实例可以用来为多个流传输服务。
 * 但是任意一个给定的流必须只能在一个client进行响应，目的是为了避免无序响应。
 *这个类用来向服务端发送请求，TransportResponseHandler用来处理服务端的响应消息。、
 这是线程安全的类，可以从多线程调用。

 */
public class TransportClient  implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(TransportClient.class);

    //netty 的IO通道
    private final Channel channel;
    //用来处理服务端的响应
    private final TransportResponseHandler handler;
    //客户端ID
    @Nullable private String clientId;
    //是否超时
    private volatile boolean timedOut;

    public Channel getChannel() {
        return channel;
    }

    public TransportResponseHandler getHandler() {
        return handler;
    }
    /**
     * Returns the ID used by the client to authenticate
     * itself when authentication is enabled.
     *
     * @return The client ID, or null if authentication is disabled.
     */
    @Nullable
    public String getClientId() {
        return clientId;
    }
    /**
     * Sets the authenticated client ID.
     * This is meant to be used by the authentication layer.
     *
     * Trying to set a different client ID after it's been set will result in an exception.
     */
    public void setClientId(@Nullable String clientId) {
        this.clientId = clientId;
    }

    public TransportClient(Channel channel, TransportResponseHandler handler) {
        this.channel = Preconditions.checkNotNull(channel);
        this.handler = Preconditions.checkNotNull(handler);
        this.timedOut = false;
    }
    public SocketAddress getSocketAddress() {
        return channel.remoteAddress();
    }

    /**
     * Requests a single chunk from the remote side,
     * from the pre-negotiated streamId.
     *
     * Chunk indices go from 0 onwards.
     * It is valid to request the same chunk multiple times, though
     * some streams may not support this.
     *
     * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
     * to be returned in the same order that they were requested, assuming only a single
     * TransportClient is used to fetch the chunks.
     *
     * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
     *                 be agreed upon by client and server beforehand.
     * @param chunkIndex 0-based index of the chunk to fetch
     * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.

    从预先协商的streamId请求远程端的单个块。
     块指示从0开始，多次请求相同的块是有效的，尽管有些流不支持这一点。
   多个fetchChunk请求可能同时出现，并且保证以与请求的顺序相同的顺序返回块，
   假设仅使用单个TransportClient来获取块。



     */
    public void fetchChunk(
            long streamId,
            final int chunkIndex,
            final ChunkReceivedCallback callback){
        final String serverAddr = NettyUtils.getRemoteAddress(channel);
        final long startTime = System.currentTimeMillis();
        logger.debug("Sending fetch chunk request {} to {}", chunkIndex, serverAddr);
        final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
        handler.addFetchRequest(streamChunkId, callback);
        channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(
                future -> {
                if(future.isSuccess()){
                    long timeTaken = System.currentTimeMillis() - startTime;
                    logger.trace("Sending request {} to {} took {} ms", streamChunkId, serverAddr,
                            timeTaken);
                }else{
                    String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
                            serverAddr, future.cause());
                    logger.error(errorMsg, future.cause());
                    handler.removeFetchRequest(streamChunkId);
                    channel.close();
                    try {
                        callback.onFailure(chunkIndex, new IOException(errorMsg, future.cause()));
                    } catch (Exception e) {
                        logger.error("Uncaught exception in RPC response callback handler!", e);
                    }
                }
            }
       );
    }
    /**
     * Request to stream the data with the given stream ID from the remote end.
     *请求远端发送流数据 ，streamId
     * @param streamId The stream to fetch.
     * @param callback Object to call with the stream data.
     */
    public void stream(final String streamId, final StreamCallback callback) {
        //远端地址
        final String serverAddr = NettyUtils.getRemoteAddress(channel);
        //用于计算时间
        final long startTime = System.currentTimeMillis();

        logger.debug("Sending stream request for {} to {}", streamId, serverAddr);
        // Need to synchronize here so that the callback is added to the queue and the RPC is
        // written to the socket atomically, so that callbacks are called in the right order
        // when responses arrive.
        /*
            这里需要同步,以便可以将回调添加到队列中。并且RPC可以原子的写入到socket
            以便在响应到达的时候回调可以以正确的顺序调用

         */
        synchronized (this) {
            //队列添加回调函数
            handler.addStreamCallback(callback);
            channel.writeAndFlush(new StreamRequest(streamId)).addListener(
                    (future) ->{
                        if (future.isSuccess()) {
                            long timeTaken = System.currentTimeMillis() - startTime;
                            logger.trace("Sending request for {} to {} took {} ms", streamId, serverAddr,
                                    timeTaken);
                        }else{
                            String errorMsg = String.format("Failed to send request for %s to %s: %s", streamId,
                                    serverAddr, future.cause());
                            logger.error(errorMsg, future.cause());
                            channel.close();
                            try {
                                callback.onFailure(streamId, new IOException(errorMsg, future.cause()));
                            } catch (Exception e) {
                                logger.error("Uncaught exception in RPC response callback handler!", e);
                            }
                        }
                    }
            );

        }
    }
    /**
     * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
     * with the server's response or upon any failure.
     *
     * @param message The message to send.
     * @param callback Callback to handle the RPC's reply.
     * @return The RPC's id.
     *
     *在服务器端发送不透明的消息给rpchandler,任何的错误都会响应调用回调函数的失败方法。
     *
     */
    public long sendRpc(ByteBuffer message, final RpcResponseCallback callback) {
        final String serverAddr = NettyUtils.getRemoteAddress(channel);
        final long startTime = System.currentTimeMillis();
        logger.trace("Sending RPC to {}", serverAddr);
        //生成requestId
        final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
        //添加rpc回调
        handler.addRpcRequest(requestId, callback);
        //写出消息给远端
        channel.writeAndFlush(new RpcRequest(requestId,new NioManagedBuffer(message))).addListener(
                (future) ->{
                    if(future.isSuccess()){//成功的处理消息 计算耗时
                        long timeTaken = System.currentTimeMillis() - startTime;
                        logger.trace("Sending request {} to {} took {} ms", requestId, serverAddr, timeTaken);
                    }else{//失败的处理
                        String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
                                serverAddr, future.cause());
                        logger.error(errorMsg, future.cause());
                        handler.removeRpcRequest(requestId);
                        channel.close();
                        try {
                            callback.onFailure(new IOException(errorMsg, future.cause()));
                        } catch (Exception e) {
                            logger.error("Uncaught exception in RPC response callback handler!", e);
                        }
                    }
                }
        );
        return requestId;
    }
    /**
     * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
     * a specified timeout for a response.
     * 同步的发送不够明的消息给服务端的RpcHandler,同步等待响应，直到超过指定的超时时间
     */
    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
        final SettableFuture<ByteBuffer> result = SettableFuture.create();
        sendRpc(message, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                ByteBuffer copy = ByteBuffer.allocate(response.remaining());
                copy.put(response);
                // flip "copy" to make it readable
                copy.flip();
                result.set(copy);
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
        try {
            return result.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    /**
     * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
     * message, and no delivery guarantees are made.
     * 发送不透明消息给服务端的RpcHandler,不需要响应 不提供交付保证
     * @param message The message to send.
     */
    public void send(ByteBuffer message) {
        channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
    }
    /**
     * Removes any state associated with the given RPC.
     *
     * @param requestId The RPC id returned by
     * {@link #//sendRpc(byte[], RpcResponseCallback)}.
     *
     *删除与给定RPC相关联的任何状态
     *
     */
    public void removeRpcRequest(long requestId) {
        handler.removeRpcRequest(requestId);
    }

    /** Mark this channel as having timed out. */
    public void timeOut() {
        this.timedOut = true;
    }
    @Override
    public void close() throws IOException {
    /* close is a local operation and should finish with milliseconds;
     timeout just to be safe
        关闭时一个本地操作，应该在毫秒内完成，设置超时仅仅为了安全起见
     */
        channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    public boolean isActive() {
        return !timedOut && (channel.isOpen() || channel.isActive());
    }
}
