package com.ning.client;

import com.google.common.annotations.VisibleForTesting;
import com.ning.protocol.*;
import com.ning.server.MessageHandler;
import com.ning.util.NettyUtils;
import com.ning.util.TransportFrameDecoder;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * Concurrency: thread safe and can be called from multiple threads.

处理服务端的响应消息，以响应从TransportClient发送的请求


 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
    private final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

    private final Channel channel;

    private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

    private final Map<Long, RpcResponseCallback> outstandingRpcs;

    private final Queue<StreamCallback> streamCallbacks;

    private volatile boolean streamActive;

    /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent.
     * 记录上次获取块或者RCP请求的时间
     * */
    private final AtomicLong timeOfLastRequestNs;

    public TransportResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingFetches = new ConcurrentHashMap<>();
        this.outstandingRpcs = new ConcurrentHashMap<>();
        this.streamCallbacks = new ConcurrentLinkedQueue<>();
        this.timeOfLastRequestNs = new AtomicLong(0);
    }
    public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
        updateTimeOfLastRequest();
        outstandingFetches.put(streamChunkId, callback);
    }

    public void removeFetchRequest(StreamChunkId streamChunkId) {
        outstandingFetches.remove(streamChunkId);
    }
    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        updateTimeOfLastRequest();
        outstandingRpcs.put(requestId, callback);
    }
    @VisibleForTesting
    public void deactivateStream() {
        streamActive = false;
    }


    public void removeRpcRequest(long requestId) {
        outstandingRpcs.remove(requestId);
    }
    public void addStreamCallback(StreamCallback callback) {
        timeOfLastRequestNs.set(System.nanoTime());
        streamCallbacks.offer(callback);
    }
    /** Handles the receipt of a single message.
     *处理单个消息的接收
     * */
    public void handle(ResponseMessage message) throws Exception{
        //获取远程通道地址
        String remoteAddress = NettyUtils.getRemoteAddress(channel);
        //块请求成功
        if (message instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
            //获取回调
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            //没有回调监听器
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} since it is not outstanding",
                        resp.streamChunkId, remoteAddress);
                //释放缓冲
                resp.body().release();
            }else{
                outstandingFetches.remove(resp.streamChunkId);
                //调用回调
                listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
                //释放缓冲
                resp.body().release();
            }
        }else if (message instanceof ChunkFetchFailure) {//块抓取失败
            ChunkFetchFailure resp = (ChunkFetchFailure) message;
            //块获取失败回调 监听器
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            //没有监听器
            if (listener == null) {
                logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
                        resp.streamChunkId, remoteAddress, resp.errorString);
            }else{
                //存在监听器 先移除
                outstandingFetches.remove(resp.streamChunkId);
                //调用监听器
                listener.onFailure(resp.streamChunkId.chunkIndex,new ChunkFetchFailureException(
                        "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString
                ));
            }
        }else if (message instanceof RpcResponse) {//如果是Rpc响应
            RpcResponse resp = (RpcResponse) message;
            //监听器
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {//没有对应监听器
                logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                        resp.requestId, remoteAddress, resp.body().size());
            } else {
                //先移除对应监听器
                outstandingRpcs.remove(resp.requestId);
                try {
                    listener.onSuccess(resp.body().nioByteBuffer());
                } finally {
                    resp.body().release();
                }
            }
        } else if (message instanceof RpcFailure){
            RpcFailure resp = (RpcFailure) message;
            RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
            if (listener == null) {
                logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                        resp.requestId, remoteAddress, resp.errorString);
            } else {
                outstandingRpcs.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        } else if(message instanceof StreamResponse){
            StreamResponse resp = (StreamResponse) message;
            StreamCallback callback = streamCallbacks.poll();//回调函数
            if (callback != null) {
                if (resp.byteCount > 0) {//如果流的内容不为空
                    StreamInterceptor interceptor = new StreamInterceptor(this,resp.streamId, resp.byteCount,callback);
                    try {
                        TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
                                channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
                        frameDecoder.setInterceptor(interceptor);
                        streamActive = true;
                    } catch (Exception e) {
                        logger.error("Error installing stream handler.", e);
                        deactivateStream();
                    }
                }else{
                    try {
                        callback.onComplete(resp.streamId);
                    } catch (Exception e) {
                        logger.warn("Error in stream handler onComplete().", e);
                    }
                }
            } else {
                logger.error("Could not find callback for StreamResponse.");
            }
        }else if (message instanceof StreamFailure) {//流读取失败
            StreamFailure resp = (StreamFailure) message;
            StreamCallback callback = streamCallbacks.poll();
            if (callback != null) {
                try {
                    callback.onFailure(resp.streamId, new RuntimeException(resp.error));
                } catch (IOException ioe) {
                    logger.warn("Error in stream failure handler.", ioe);
                }
            }else{
                logger.warn("Stream failure with unknown callback: {}", resp.error);
            }
        } else {
            throw new IllegalStateException("Unknown response type: " + message.type());
        }

    }

    /** Invoked when an exception was caught on the Channel.
     *在channel捕获到异常的时候调用
     * */
    public void exceptionCaught(Throwable cause){
        if (numOutstandingRequests() > 0) {
            String remoteAddress = NettyUtils.getRemoteAddress(channel);
            logger.error("Still have {} requests outstanding when connection from {} is closed",
                    numOutstandingRequests(), remoteAddress);
            failOutstandingRequests(cause);
        }
    }

    /** Returns total number of outstanding requests (fetch requests + rpcs)
     * 返回为完成处理的请求
     * */
    public int numOutstandingRequests() {
        return outstandingFetches.size() + outstandingRpcs.size() + streamCallbacks.size() +
                (streamActive ? 1 : 0);
    }
    /** Invoked when the channel this MessageHandler is on has been unregistered.
     *当MessageHandler所在频道注销的时候调用
     * */
    public void channelUnregistered(){
        if (numOutstandingRequests() > 0) {
            String remoteAddress = NettyUtils.getRemoteAddress(channel);
            logger.error("Still have {} requests outstanding when connection from {} is closed",
                    numOutstandingRequests(), remoteAddress);
            failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
        }
    }

    /**
     * Fire the failure callback for all outstanding requests. This is called when we have an
     * uncaught exception or pre-mature connection termination.


     */
    private void failOutstandingRequests(Throwable cause) {
        for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
            entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
        }
        for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
            entry.getValue().onFailure(cause);
        }

        // It's OK if new fetches appear, as they will fail immediately.
        outstandingFetches.clear();
        outstandingRpcs.clear();
    }
    /** Returns the time in nanoseconds of when the last request was sent out. */
    public long getTimeOfLastRequestNs() {
        return timeOfLastRequestNs.get();
    }

    /** Updates the time of the last request to the current system time. */
    public void updateTimeOfLastRequest() {
        timeOfLastRequestNs.set(System.nanoTime());
    }

}
