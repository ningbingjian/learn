package com.ning.server;

import com.google.common.base.Throwables;
import com.ning.buffer.ManagedBuffer;
import com.ning.buffer.NioManagedBuffer;
import com.ning.client.RpcResponseCallback;
import com.ning.client.TransportClient;
import com.ning.protocol.*;
import com.ning.util.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * A handler that processes requests from clients and writes chunk data back.
 * Each handler is
 * attached to a single Netty channel,
 * and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link //TransportServer}.
 处理从客户端发来的请求和写数据块回去给客户端。
 每一handler都和单一的netty channel相关联，并且保持追踪每一个从当前通道获取的块。
 以便能在通道关闭的时候终止这些流数据块
 这些消息都是被服务端的pipeline管道处理过的。
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
    private final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

    /** The Netty channel that this handler is associated with. */
    private final Channel channel;

    /** Client on the same channel allowing
     * us to talk back to the requester.
     *管道上的客户端，允许用来响应请求者
     * */
    private final TransportClient reverseClient;

    /** Handles all RPC messages. 、
     * RPC消息处理器
     * */
    private final RpcHandler rpcHandler;

    /** Returns each chunk part of a stream.
     * 流管理器 负责返回流的每一个块
     * */
    private final StreamManager streamManager;
    public TransportRequestHandler(
            Channel channel,
            TransportClient reverseClient,
            RpcHandler rpcHandler) {
        this.channel = channel;
        this.reverseClient = reverseClient;
        this.rpcHandler = rpcHandler;
        this.streamManager = rpcHandler.getStreamManager();
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        rpcHandler.exceptionCaught(cause, reverseClient);
    }

    @Override
    public void handle(RequestMessage request) {
        if (request instanceof ChunkFetchRequest) {
            processFetchRequest((ChunkFetchRequest) request);
        } else if (request instanceof RpcRequest) {
            processRpcRequest((RpcRequest) request);
        } else if (request instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage) request);
        } else if (request instanceof StreamRequest) {
            processStreamRequest((StreamRequest) request);
        } else {
            throw new IllegalArgumentException("Unknown request type: " + request);
        }
    }
    //处理块请求
    private void processFetchRequest(final ChunkFetchRequest req) {
        //客户端地址
        final String client = NettyUtils.getRemoteAddress(channel);
        //
        logger.trace("Received req from {} to fetch block {}", client, req.streamChunkId);
        ManagedBuffer buf;
        try {
            //认证
            streamManager.checkAuthorization(reverseClient,req.streamChunkId.streamId);
            //注册管道
            streamManager.registerChannel(channel, req.streamChunkId.streamId);
            //返回对应块
            buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);


        } catch (Exception e) {
            logger.error(String.format(
                    "Error opening block %s for request from %s", req.streamChunkId, client), e);
            respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
            return;
        }
        respond(new ChunkFetchSuccess(req.streamChunkId, buf));

    }
    //rpc消息处理
    private void processRpcRequest(final RpcRequest req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            });
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        } finally {
            req.body().release();
        }
    }
    //单向消息处理
    private void processOneWayMessage(OneWayMessage req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
        } finally {
            req.body().release();
        }
    }
    //流响应处理
    private void processStreamRequest(final StreamRequest req) {
        final String client = NettyUtils.getRemoteAddress(channel);
        ManagedBuffer buf;
        try {
            buf = streamManager.openStream(req.streamId);
        } catch (Exception e) {
            logger.error(String.format(
                    "Error opening stream %s for request from %s", req.streamId, client), e);
            respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
            return;
        }

        respond(new StreamResponse(req.streamId, buf.size(), buf));
    }



    @Override
    public void channelUnregistered() {
        if (streamManager != null) {
            try {
                streamManager.connectionTerminated(channel);
            } catch (RuntimeException e) {
                logger.error("StreamManager connectionTerminated() callback failed.", e);
            }
        }
        rpcHandler.connectionTerminated(reverseClient);
    }
    /**
     * Responds to a single message with some Encodable object.
     * If a failure occurs while sending,
     * it will be logged and the channel closed.
     * 用Encodable对象响应单个消息，如果在发送的时候出现失败，打印日志和关闭通道
     */
    private void respond(final Encodable result) {
        final String remoteAddress = channel.remoteAddress().toString();
        channel.writeAndFlush(result).addListener(
                new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            logger.trace(String.format("Sent result %s to client %s", result, remoteAddress));
                        } else {
                            logger.error(String.format("Error sending result %s to %s; closing connection",
                                    result, remoteAddress), future.cause());
                            channel.close();
                        }
                    }
                }
        );
    }


}
