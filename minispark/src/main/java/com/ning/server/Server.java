package com.ning.server;

import io.netty.channel.EventLoopGroup;

/**
 * Created by zhaoshufen on 2017/3/14.
 */
public class Server {
    public static void main(String[] args) {
        EventLoopGroup bossGroup =
            NettyUtils.createEventLoop(ioMode, conf.serverThreads(), "shuffle-server");
        EventLoopGroup workerGroup = bossGroup;
    }
}
