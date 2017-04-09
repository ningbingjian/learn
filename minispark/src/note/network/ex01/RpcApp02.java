package ex01;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoshufen on 2017/4/8.
 */
public class RpcApp02 {
    static TransportServer server;
    static TransportClientFactory clientFactory;
    static RpcHandler rpcHandler;
    static List<String> oneWayMsgs;
    static{
        rpcHandler = new RpcHandler() {
            @Override
            public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
                String msg = JavaUtils.bytesToString(message);
                String resMsg =  String.format("i receive msg:%s" ,msg);
                System.out.println("server:"+resMsg);
                callback.onSuccess(JavaUtils.stringToBytes(resMsg));
            }

            @Override
            public StreamManager getStreamManager() {
                return new OneForOneStreamManager();
            }

            @Override
            public void receive(TransportClient client, ByteBuffer message) {
                String msg = JavaUtils.bytesToString(message);
                String resMsg =  String.format("i receive one way msg:%s" ,msg);
                System.out.println(resMsg);
            }
        };
        TransportConf conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
        TransportContext context = new TransportContext(conf,rpcHandler);
        clientFactory = context.createClientFactory();
        server = context.createServer();
    }

    public static void main(String[] args)throws Exception {
        final String command = "hello/allan";
        String addr = InetAddress.getLocalHost().getHostAddress();
        TransportClient client = clientFactory.createClient(addr,server.getPort());
        final Semaphore sem = new Semaphore(0);
        client.sendRpc(JavaUtils.stringToBytes(command), new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                System.out.println(JavaUtils.bytesToString(response));
                sem.release();
            }

            @Override
            public void onFailure(Throwable e) {
                System.out.println(e.getMessage());
                sem.release();
            }
        });
        //等待
        if (!sem.tryAcquire(1, 5, TimeUnit.SECONDS)) {
            throw  new RuntimeException("Timeout getting response from the server");
        }
    }
}
