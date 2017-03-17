package use01;

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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ning on 2017/3/17.
 */
public class RpcServer {
    private TransportServer server;
    private TransportClientFactory clientFactory;
    private RpcHandler rpcHandler;
    private List<String> oneWayMsgs;
    public void start(){
        //创建配置类
        TransportConf conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
        rpcHandler = new RpcHandler() {
            /**
             *
             * 带响应的消息接收
             * client -->客户端
             * message  客户端发送过来的消息
             * callback -->回调函数 成功之后客户端回调
             */
            public  void receive(
                    TransportClient client,
                    ByteBuffer message,
                    RpcResponseCallback callback){
                //解析字节到字符串
                String msg = JavaUtils.bytesToString(message);
                //

            }

            /**
             * 接收客户端消息
             * 没有响应 解析到消息立刻结束
             * @param client A channel client which enables the handler to make requests back to the sender
             *               of this RPC. This will always be the exact same object for a particular channel.
             * @param message The serialized bytes of the RPC.
             */
            @Override
            public void receive(TransportClient client, ByteBuffer message) {
                oneWayMsgs.add(JavaUtils.bytesToString(message));
            }
            @Override
            public StreamManager getStreamManager() { return new OneForOneStreamManager(); }
            //上下文  负责创建服务端 、负责创建客户端工厂  进而创建客户端


        };
        TransportContext context = new TransportContext(conf, rpcHandler);
        server = context.createServer();
        clientFactory = context.createClientFactory();
        //保存消息用 测试方便
        oneWayMsgs = new ArrayList<>();

    }
    public void stop(){
        server.close();
        clientFactory.close();
    }

    public static void main(String[] args) {
        new RpcServer().start();
        Thread t = new Thread(() ->{
            for(;;){

            }
        });
        t.start();
    }
}
