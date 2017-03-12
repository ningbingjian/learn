package com.ning.net.common.server;


import org.apache.spark.network.util.IOMode;

/**
 * Created by zhaoshufen on 2017/3/12.
 */
public class TransportServer {
    public TransportServer(String host,int port){
        this.init(host,port);
    }
    private void init(String hostToBind, int portToBind) {
        IOMode ioMode = IOMode.valueOf("NIO");

    }
}
