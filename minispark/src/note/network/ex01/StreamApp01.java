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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 测试从服务端获取流
 */
public class StreamApp01 {

    static TransportServer server;
    static TransportClientFactory clientFactory;
    static RpcHandler rpcHandler;
    static String requestStreamId = "StreamApp01";
    static Semaphore sem = new Semaphore(0);
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
        server = context.createServer();
        clientFactory = context.createClientFactory();
    }

    public static void main(String[] args)throws Exception {
        TransportClient client = clientFactory.createClient(getLocalHost(),server.getPort());
        final  FileChannel fileChannel =  new RandomAccessFile("pom1.xml","rw").getChannel();
        client.stream(requestStreamId, new StreamCallback() {
            int count = 0 ;
            @Override
            public void onData(String streamId, ByteBuffer buf) throws IOException {
                fileChannel.write(buf);
                count += buf.limit();
                String msg = String.format("客户端:onData,%s,内容:%s",streamId,buf.limit());
                System.out.println(msg);
               // String msg = String.format("客户端：接收到服务端的响应,data.length=%d",tmp.length);
                //System.out.println(msg);

            }

            @Override
            public void onComplete(String streamId) throws IOException {
                System.out.println("客户端:onComplete -->" + count);
                fileChannel.close();
                sem.release();

            }

            @Override
            public void onFailure(String streamId, Throwable cause) throws IOException {
                String msg = String.format("客户端:onFailure,%s,errormsg:%s",streamId,cause.getMessage());
                sem.release();
            }
        });
        if(!sem.tryAcquire(1,120, TimeUnit.SECONDS)){
            throw new RuntimeException("timeout");
        }
        client.close();


    }
    private static ByteBuffer createBuffer(int bufSize) {
        try {
            File file = new File("pom.xml");
            FileChannel fileChannel = new RandomAccessFile(file,"r").getChannel();
            ByteBuffer buf =  fileChannel.map(FileChannel.MapMode.READ_ONLY,0,file.length());
            System.out.println("服务端:buf.limit()"+buf.limit());
            return buf;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

        }
    }
    public static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
