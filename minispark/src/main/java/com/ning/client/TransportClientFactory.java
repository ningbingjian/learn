package com.ning.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.ning.TransportContext;
import com.ning.server.TransportChannelHandler;
import com.ning.util.IOMode;
import com.ning.util.JavaUtils;
import com.ning.util.NettyUtils;
import com.ning.util.TransportConf;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 */
public class TransportClientFactory implements Closeable {
    /** A simple data structure to track the pool of clients between two peer nodes.
     * 一个追踪两个对等节点的客户端池的简单的数据结构
     * */
    private static class ClientPool{
        TransportClient[] clients;
        Object[] locks;
        public ClientPool(int size) {
            clients = new TransportClient[size];
            locks = new Object[size];
            for (int i = 0; i < size; i++) {
                locks[i] = new Object();
            }
        }

    }
    private final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);
    private final TransportContext context;
    private final TransportConf conf;
    private final List<TransportClientBootstrap> clientBootstraps;
    private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

    /**
     * Random number generator for picking connections between peers.
     *
     * */
    private final Random rand;
    private final int numConnectionsPerPeer;

    private final Class<? extends Channel> socketChannelClass;
    private EventLoopGroup workerGroup;
    private PooledByteBufAllocator pooledAllocator;
    public TransportClientFactory(
            TransportContext context,
            List<TransportClientBootstrap> clientBootstraps) {
        //context
        this.context = Preconditions.checkNotNull(context);
        //TransportConf
        this.conf = context.getConf();
        this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
        //连接池 每个socket地址对应一个池
        this.connectionPool = new ConcurrentHashMap<SocketAddress, ClientPool>();
        //每个对等节点对应的连接数量 spark.shuffle.io.numConnectionsPerPeer 默认是1
        this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
        //随机数
        this.rand = new Random();
        //默认是nio模型 spark.module.io.mode  -->module是模块 在构建TransportConf传入
        IOMode ioMode = IOMode.valueOf(conf.ioMode());

        this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
        // TODO: Make thread pool name configurable.
        this.workerGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "shuffle-client");
        //spark.module.io.preferDirectBufs
        //spark.module.io.clientThreads
        this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
                conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
    }

    /**
     * Create a {@link TransportClient} connecting to the given remote host / port.
     *
     * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
     * and randomly picks one to use. If no client was previously created in the randomly selected
     * spot, this function creates a new client and places it there.
     *
     * Prior to the creation of a new TransportClient, we will execute all
     * {@link TransportClientBootstrap}s that are registered with this factory.
     *
     * This blocks until a connection is successfully established and fully bootstrapped.
     *
     * Concurrency: This method is safe to call from multiple threads.
     *
     * 创建TransportClient,用于连接远程服务器
     *我们保持一组客户端【数组大小由spark.shuffle.io.numConnectionsPerPeer设置】并且随机选择一个使用.
     * 如果在原来的客户端随机选择槽没有客户端，那就创建一个新的客户端，并且放到数组
     *
     *优先创建新的TransportClient,将会执行所有注册到factory的TransportClientBootstrap
     * 并发：这这个方法是线程安全的，可以多个线程同时调用。
     */
    public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
        // Get connection from the connection pool first.
        // If it is not found or not active, create a new one.
        //首先从连接池获取连接，如果没有从池中找到链接或者没有激活，创建一个新的client
        final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
        //从连接池获取连接
        ClientPool clientPool = connectionPool.get(address);
        //如果没有找到对应连接池,放一个该地址对应的连接池
        if (clientPool == null) {
            connectionPool.putIfAbsent(address, new ClientPool(numConnectionsPerPeer));
            clientPool = connectionPool.get(address);
        }
        //随机选择一个client
        int clientIndex = rand.nextInt(numConnectionsPerPeer);
        TransportClient cachedClient = clientPool.clients[clientIndex];
        //如果这个位置没有对应client
        if (cachedClient != null && cachedClient.isActive()) {
            // Make sure that the channel will not timeout by updating the last use time of the
            // handler. Then check that the client is still alive, in case it timed out before
            // this code was able to update things.
            /**
             * 通过更新最后一次使用时间确保通道没有超时。然后检查客户端是否仍然存在，以防在代码能更新之前超时
             */
            TransportChannelHandler handler = cachedClient.getChannel().pipeline()
                    .get(TransportChannelHandler.class);
            synchronized (handler) {
                handler.getResponseHandler().updateTimeOfLastRequest();
            }
            //返回client
            if (cachedClient.isActive()) {
                logger.trace("Returning cached connection to {}: {}", address, cachedClient);
                return cachedClient;
            }

        }
        // If we reach here, we don't have an existing connection open. Let's create a new one.
        // Multiple threads might race here to create new connections. Keep only one of them active.
        /*
            如果能到这里，那就是没有打开过连接。创建一个新的连接。可能会有多线程竞争创建连接。保持只有一个线程来创建连接

         */
        synchronized (clientPool.locks[clientIndex]) {
            cachedClient = clientPool.clients[clientIndex];
            if (cachedClient != null) {
                if (cachedClient.isActive()) {
                    logger.trace("Returning cached connection to {}: {}", address, cachedClient);
                    return cachedClient;
                } else {
                    logger.info("Found inactive connection to {}, creating a new one.", address);
                }
            }
            clientPool.clients[clientIndex] = createClient(address);
            return clientPool.clients[clientIndex];
        }
    }
    /**
     * Create a completely new {@link TransportClient} to the given remote host / port.
     * This connection is not pooled.
     *
     * As with {@link #createClient(String, int)}, this method is blocking.
     */
    public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
            throws IOException {
        final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
        return createClient(address);
    }

    /** Create a completely new {@link TransportClient} to the remote address.
     *创建一个全新的TransportClient
     * */
    private TransportClient createClient(InetSocketAddress address) throws IOException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(socketChannelClass)
                // Disable Nagle's Algorithm since we don't want packets to wait
                //禁用Nagle算法，因为不需要数据包等待
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                //spark.network.timeout 连接超时 默认120妙
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
                .option(ChannelOption.ALLOCATOR, pooledAllocator);
        final AtomicReference<TransportClient> clientRef = new AtomicReference<TransportClient>();
        final AtomicReference<Channel> channelRef = new AtomicReference<Channel>();
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                TransportChannelHandler clientHandler = context.initializePipeline(ch);
                clientRef.set(clientHandler.getClient());
                channelRef.set(ch);
            }
        });
        // Connect to the remote server
        long preConnect = System.nanoTime();
        ChannelFuture cf = bootstrap.connect(address);
        if (!cf.awaitUninterruptibly(conf.connectionTimeoutMs())) {
            throw new IOException(
                    String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
        } else if (cf.cause() != null) {
            throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
        }

        TransportClient client = clientRef.get();
        Channel channel = channelRef.get();
        assert client != null : "Channel future completed successfully with null client";

        // Execute any client bootstraps synchronously before marking the Client as successful.
        long preBootstrap = System.nanoTime();
        logger.debug("Connection to {} successful, running bootstraps...", address);
        try {
            for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
                clientBootstrap.doBootstrap(client, channel);
            }
        } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
            long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
            logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
            client.close();
            throw Throwables.propagate(e);
        }
        long postBootstrap = System.nanoTime();

        logger.debug("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
                address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);
        return client;
    }
    @Override
    public void close() throws IOException {
        // Go through all clients and close them if they are active.
        //释放连接池
        for (ClientPool clientPool : connectionPool.values()) {
            for (int i = 0; i < clientPool.clients.length; i++) {
                TransportClient client = clientPool.clients[i];
                if (client != null) {
                    clientPool.clients[i] = null;
                    JavaUtils.closeQuietly(client);
                }
            }
        }

        connectionPool.clear();
        //工作线程组

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }
}
