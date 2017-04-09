import com.google.common.collect.Sets;
import com.ning.TransportContext;
import com.ning.client.RpcResponseCallback;
import com.ning.client.TransportClient;
import com.ning.client.TransportClientFactory;
import com.ning.server.OneForOneStreamManager;
import com.ning.server.RpcHandler;
import com.ning.server.StreamManager;
import com.ning.server.TransportServer;
import com.ning.util.JavaUtils;
import com.ning.util.SystemPropertyConfigProvider;
import com.ning.util.TransportConf;
import org.apache.spark.network.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
/**
 * Created by zhaoshufen on 2017/4/8.
 */
public class RcpIntegrationSuite {
    static TransportConf transportConf;
    static TransportServer server;
    static TransportClientFactory clientFactory;
    static RpcHandler rpcHandler;
    static List<String> oneWayMsgs;

    @BeforeClass
    public static void setUp(){
        System.out.println("before");
        transportConf = new TransportConf("shuffle",new SystemPropertyConfigProvider());
        rpcHandler = new RpcHandler() {
            @Override
            public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
                String msg = JavaUtils.bytesToString(message);
                String [] parts = msg.split("/");
                if(parts[0].equals("hello")){
                    callback.onSuccess(JavaUtils.stringToBytes("Hello, " + parts[1] + "!"));
                } else if (parts[0].equals("return error")) {
                    callback.onFailure(new RuntimeException("Returned: " + parts[1]));
                } else if (parts[0].equals("throw error")) {
                    throw new RuntimeException("Thrown: " + parts[1]);
                }
            }

            @Override
            public StreamManager getStreamManager() {
                return new OneForOneStreamManager();
            }

            @Override
            public void receive(TransportClient client, ByteBuffer message) {
                oneWayMsgs.add(JavaUtils.bytesToString(message));
            }
        };
        TransportContext context = new TransportContext(transportConf, rpcHandler);
        server = context.createServer();
        clientFactory = context.createClientFactory();
        oneWayMsgs = new ArrayList<>();
    }
    class RpcResult{
        private Set<String> successMessages;
        private Set<String> errorMessages;
    }
    private RpcResult sendRPC(String...commands)throws Exception{
        TransportClient client = clientFactory.createClient(TestUtils.getLocalHost(), server.getPort());
        final Semaphore sem = new Semaphore(0);
        final RpcResult res = new RpcResult();
        res.successMessages = Collections.synchronizedSet(new HashSet<String>());
        res.errorMessages = Collections.synchronizedSet(new HashSet<String>());
        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onFailure(Throwable e) {
                res.errorMessages.add(e.getMessage());
                sem.release();
            }

            @Override
            public void onSuccess(ByteBuffer response) {
                res.successMessages.add(JavaUtils.bytesToString(response));
                sem.release();
            }
        };
        for(String command : commands){
           client.sendRpc(JavaUtils.stringToBytes(command),callback);
        }
        if(!sem.tryAcquire(commands.length,5, TimeUnit.SECONDS)){
            fail("Timeout getting response from the server");
        }
        client.close();
        return res;
    }
    @Test
    public void singleRpc() throws Exception{
        RpcResult res = sendRPC("hello/Aaron");
        assertEquals(res.successMessages, Sets.newHashSet("Hello, Aaron!"));
        assertTrue(res.errorMessages.isEmpty());
    }
    @AfterClass
    public static void tearDown(){
        System.out.println("tearDown");
    }

}
