package com.ning.server;

import com.ning.client.TransportClient;
import com.ning.client.TransportResponseHandler;
import com.ning.protocol.Message;
import com.ning.protocol.RequestMessage;
import com.ning.protocol.ResponseMessage;
import com.ning.util.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single Transport-level Channel handler which is used for delegating requests to the
 * {@link TransportRequestHandler} and responses to the {@link TransportResponseHandler}.
 *
 * All channels created in the transport layer are bidirectional. When the Client initiates a Netty
 * Channel with a RequestMessage (which gets handled by the Server's RequestHandler), the Server
 * will produce a ResponseMessage (handled by the Client's ResponseHandler). However, the Server
 * also gets a handle on the same Channel, so it may then begin to send RequestMessages to the
 * Client.
 * This means that the Client also needs a RequestHandler and the Server needs a ResponseHandler,
 * for the Client's responses to the Server's requests.
 *
 * This class also handles timeouts from a {@link io.netty.handler.timeout.IdleStateHandler}.
 * We consider a connection timed out if there are outstanding fetch or RPC requests but no traffic
 * on the channel for at least `requestTimeoutMs`. Note that this is duplex traffic; we will not
 * timeout if the client is continuously sending but getting no responses, for simplicity.


 */
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
    private final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

    //客户端
    private final TransportClient client;
    //响应客户端的handler
    private final TransportResponseHandler responseHandler;
    //请求服务端的handler
    private final TransportRequestHandler requestHandler;
    //请求超时使劲
    private final long requestTimeoutNs;
    //关闭空闲连接
    private final boolean closeIdleConnections;
    public TransportChannelHandler(
            TransportClient client,
            TransportResponseHandler responseHandler,
            TransportRequestHandler requestHandler,
            long requestTimeoutMs,
            boolean closeIdleConnections) {
        this.client = client;
        this.responseHandler = responseHandler;
        this.requestHandler = requestHandler;
        this.requestTimeoutNs = requestTimeoutMs * 1000L * 1000;
        this.closeIdleConnections = closeIdleConnections;
    }

    public TransportClient getClient() {
        return client;
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Exception in connection from " + NettyUtils.getRemoteAddress(ctx.channel()),
                cause);
        requestHandler.exceptionCaught(cause);
        responseHandler.exceptionCaught(cause);
        ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelUnregistered();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while unregistering channel", e);
        }
        try {
            responseHandler.channelUnregistered();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while unregistering channel", e);
        }
        super.channelUnregistered(ctx);
    }
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Message request) throws Exception {
        if (request instanceof RequestMessage) {
            requestHandler.handle((RequestMessage) request);
        } else {
            responseHandler.handle((ResponseMessage) request);
        }
    }
    /** Triggered based on events from an {@link io.netty.handler.timeout.IdleStateHandler}.
     *根据IdleStateHandler的时间触发
     *
     * */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            // See class comment for timeout semantics. In addition to ensuring we only timeout while
            // there are outstanding requests, we also do a secondary consistency check to ensure
            // there's no race between the idle timeout and incrementing the numOutstandingRequests
            // (see SPARK-7003).
            //
            // To avoid a race between TransportClientFactory.createClient() and this code which could
            // result in an inactive client being returned, this needs to run in a synchronized block.
            /**
             * 查看类的超时语义。
             * 除了确保我们未完成请求的空间超时，我们还进行了二次一致性检查，确保空闲超时和未完成的请求之间存在竞争
             * 为了避免TransportClientFactory.createClient()和可以导致未激活客户端返回的代码之间产生竞争，这里的代码
             * 需要运行在同步块中。
             *
             */
            synchronized (this) {
                //判断是否超时
                boolean isActuallyOverdue =
                        System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
                if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
                    if (responseHandler.numOutstandingRequests() > 0) {
                        String address = NettyUtils.getRemoteAddress(ctx.channel());
                        logger.error("Connection to {} has been quiet for {} ms while there are outstanding " +
                                "requests. Assuming connection is dead; please adjust spark.network.timeout if this " +
                                "is wrong.", address, requestTimeoutNs / 1000 / 1000);
                        //设置客户端超时
                        client.timeOut();
                        ctx.close();
                    }
                }
            }
        }
    }

    public TransportResponseHandler getResponseHandler() {
        return responseHandler;
    }
}
