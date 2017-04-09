package ex01;

import com.google.common.collect.Sets;
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
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by zhaoshufen on 2017/4/8.
 */
public class RpcApp01 {
    static TransportServer server;
    static TransportClientFactory clientFactory;
    static RpcHandler rpcHandler;
    static List<String> oneWayMsgs;
    static {

        TransportConf conf = new TransportConf("shuffle", new SystemPropertyConfigProvider());
        rpcHandler = new RpcHandler() {
            @Override
            public void receive(
                    TransportClient client,
                    ByteBuffer message,
                    RpcResponseCallback callback) {
                String msg = JavaUtils.bytesToString(message);
                String[] parts = msg.split("/");
                if (parts[0].equals("hello")) {
                    callback.onSuccess(JavaUtils.stringToBytes("Hello, " + parts[1] + "!"));
                } else if (parts[0].equals("return error")) {
                    callback.onFailure(new RuntimeException("Returned: " + parts[1]));
                } else if (parts[0].equals("throw error")) {
                    throw new RuntimeException("Thrown: " + parts[1]);
                }
            }

            @Override
            public void receive(TransportClient client, ByteBuffer message) {
                oneWayMsgs.add(JavaUtils.bytesToString(message));
            }

            @Override
            public StreamManager getStreamManager() { return new OneForOneStreamManager(); }
        };
        TransportContext context = new TransportContext(conf, rpcHandler);
        server = context.createServer();
        clientFactory = context.createClientFactory();
        oneWayMsgs = new ArrayList<>();
    }
    class RpcResult {
        public Set<String> successMessages;
        public Set<String> errorMessages;
    }
    public  RpcResult sendRPC(String ... commands) throws Exception {
        TransportClient client = clientFactory.createClient(getLocalHost(), server.getPort());
        final Semaphore sem = new Semaphore(0);

        final RpcResult res = new RpcResult();
        res.successMessages = Collections.synchronizedSet(new HashSet<>());
        res.errorMessages = Collections.synchronizedSet(new HashSet<String>());

        RpcResponseCallback callback = new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer message) {
            String response = JavaUtils.bytesToString(message);
            res.successMessages.add(response);
            sem.release();
          }

          @Override
          public void onFailure(Throwable e) {
            res.errorMessages.add(e.getMessage());
            sem.release();
          }
        };

        for (String command : commands) {
          client.sendRpc(JavaUtils.stringToBytes(command), callback);
        }

        if (!sem.tryAcquire(commands.length, 5, TimeUnit.SECONDS)) {
          throw  new RuntimeException("Timeout getting response from the server");
        }
        client.close();
        return res;
  }
    public static void main(String[] args) throws Exception{
        singleRpc();
    }

    private static void singleRpc() throws Exception {
        RpcResult res = new RpcApp01().sendRPC("hello/Aaron");
        System.out.println(res.successMessages);
        System.out.println(res.errorMessages);
        assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
        assertTrue(res.errorMessages.isEmpty());
    }

    public static String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
