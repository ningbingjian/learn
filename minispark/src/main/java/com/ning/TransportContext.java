package com.ning;

import com.google.common.collect.Lists;
import com.ning.client.TransportClient;
import com.ning.client.TransportClientBootstrap;
import com.ning.client.TransportClientFactory;
import com.ning.client.TransportResponseHandler;
import com.ning.protocol.MessageDecoder;
import com.ning.protocol.MessageEncoder;
import com.ning.server.*;
import com.ning.util.NettyUtils;
import com.ning.util.TransportConf;
import com.ning.util.TransportFrameDecoder;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Contains the context to create a
 * {@link //TransportServer}, {@link //TransportClientFactory}, and to
 * setup Netty Channel pipelines with a {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.

  包含创建TransportServer,TransportClientFactory的上下文，并使用TransportChannelHandler设置netty channel管道
 TransportClient 提供两种通信协议，rpc控制协议和块抓取控制协议。
 Rpc的处理在TransportContext之外[即由用户提供的handler处理]，并且它负责设置使用零拷贝IO块形式的流传输。
 TransportServer和TransportClientFactory都会为管道channel创建TransportChannelHandler
 因为每个TransportChannelHandler都包含TransportClient,这样可以使服务器在现有的通道上将消息发送回给客户端


 */
public class TransportContext {
    private final Logger logger = LoggerFactory.getLogger(TransportContext.class);
    //和网络传输有关的配置
    private final TransportConf conf;
    private final RpcHandler rpcHandler;

    private final boolean closeIdleConnections;

    private final MessageEncoder encoder;
    private final MessageDecoder decoder;

    public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
        this(conf, rpcHandler, false);
    }

    public TransportContext(
            TransportConf conf,
            RpcHandler rpcHandler,
            boolean closeIdleConnections) {
        this.conf = conf;
        this.rpcHandler = rpcHandler;
        this.encoder = new MessageEncoder();
        this.decoder = new MessageDecoder();
        this.closeIdleConnections = closeIdleConnections;
    }

    public TransportConf getConf() {
        return conf;
    }
    /**
     * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
     * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
     * to create a Client.
     */
    public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
        return new TransportClientFactory(this, bootstraps);
    }

    public TransportClientFactory createClientFactory() {
        return createClientFactory(Lists.<TransportClientBootstrap>newArrayList());
    }
    /** Create a server which will attempt to bind to a specific port. */
    public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, null, port, rpcHandler, bootstraps);
    }

    /** Create a server which will attempt to bind to a specific host and port. */
    public TransportServer createServer(
            String host, int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, host, port, rpcHandler, bootstraps);
    }

    /** Creates a new server, binding to any available ephemeral port. */
    public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
        return createServer(0, bootstraps);
    }

    public TransportServer createServer() {
        return createServer(0, Lists.<TransportServerBootstrap>newArrayList());
    }

    public TransportChannelHandler initializePipeline(SocketChannel channel) {
        return initializePipeline(channel, rpcHandler);
    }
    /**
     * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
     * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
     * response messages.
     *
     * @param channel The channel to initialize.
     * @param channelRpcHandler The RPC handler to use for the channel.
     *
     * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
     * be used to communicate on this channel. The TransportClient is directly associated with a
     * ChannelHandler to ensure all users of the same channel get the same TransportClient object.

        流入的消息解码路线:TransportFrameDecoder-->IdleStateHandler-->channelHandler -->TransportRequestHandler
        流出的消息编码路线:TransportResponseHandler-->IdleStateHandler-->encoder-->
     */
    public TransportChannelHandler initializePipeline(
            SocketChannel channel,
            RpcHandler channelRpcHandler) {
        try {
            TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
            channel.pipeline()
                    .addLast("encoder", encoder)
                    .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
                    .addLast("decoder", decoder)
                    .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
                    // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
                    // would require more logic to guarantee if this were not part of the same event loop.
                    .addLast("handler", channelHandler);
            return channelHandler;
        } catch (RuntimeException e) {
            logger.error("Error while initializing Netty pipeline", e);
            throw e;
        }
    }
    /**
     * Creates the server- and client-side handler which is used to handle both RequestMessages and
     * ResponseMessages. The channel is expected to have been successfully created, though certain
     * properties (such as the remoteAddress()) may not be available yet.
     */
    private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
        TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
        TransportClient client = new TransportClient(channel, responseHandler);
        TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
                rpcHandler);
        return new TransportChannelHandler(client, responseHandler, requestHandler,
                conf.connectionTimeoutMs(), closeIdleConnections);
    }

}
