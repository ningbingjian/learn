package ex01;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;

/**
 * Created by zhaoshufen on 2017/4/9.
 */
public class StreamApp01 {

    static TransportServer server;
    static TransportClientFactory clientFactory;
    static RpcHandler rpcHandler;
    static String requestStreamId = "StreamApp01";
    static {
        //创建测试流
        ByteBuffer buffer = createBuffer(1024);
        //流管理器
        StreamManager streamManager = new StreamManager() {
            @Override
            public ManagedBuffer getChunk(long streamId, int chunkIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ManagedBuffer openStream(String streamId) {
                if(streamId.equals(requestStreamId)){
                    return new NioManagedBuffer(buffer);
                }
                throw  new RuntimeException("not have the streamId:"+streamId);
            }
        };
        rpcHandler = new RpcHandler() {
            @Override
            public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
                throw new UnsupportedOperationException("un support operation");
            }

            @Override
            public StreamManager getStreamManager() {
                return streamManager;
            }
        };
        TransportConf conf = new TransportConf("shuffle",new SystemPropertyConfigProvider());
        TransportContext context = new TransportContext(conf,rpcHandler);
        context.createServer();
        clientFactory = context.createClientFactory();
    }

    public static void main(String[] args)throws Exception {
        TransportClient client = clientFactory.createClient(getLocalHost(),server.getPort());
        client.stream(requestStreamId, new StreamCallback() {
            @Override
            public void onData(String streamId, ByteBuffer buf) throws IOException {
                byte[] tmp = new byte[buf.remaining()];
                buf.get(tmp);
                String msg = String.format("客户端：接收到服务端的响应,data.length=%d",tmp.length);
                System.out.println(msg);
            }

            @Override
            public void onComplete(String streamId) throws IOException {
                String msg = String.format("客户端:onComplete,%s",streamId);
                System.out.println(msg);
            }

            @Override
            public void onFailure(String streamId, Throwable cause) throws IOException {
                String msg = String.format("客户端:onFailure,%s,errormsg:%s",streamId,cause.getMessage());
            }
        });


    }
    private static ByteBuffer createBuffer(int bufSize) {
        ByteBuffer buf = ByteBuffer.allocate(bufSize);
        for (int i = 0; i < bufSize; i ++) {
            buf.put((byte) i);
        }
        buf.flip();
        return buf;
    }
    public static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
