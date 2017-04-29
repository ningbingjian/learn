package com.ning.server;

import com.google.common.base.Preconditions;
import com.ning.buffer.ManagedBuffer;
import com.ning.client.TransportClient;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;,
 * which are individually
 * fetched as chunks by the client. Each registered buffer is one chunk.
 流管理器，可以使用迭代器注册。可以被客户端单独作为块进行抓取，
 每一个注册的buffer就是一个块。

 */
public class OneForOneStreamManager extends StreamManager {
    private final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);
    private final AtomicLong nextStreamId;
    private final ConcurrentHashMap<Long, StreamState> streams;
    /** State of a single stream.
     * 单个流的状态描述
     * */
    private static class StreamState {
        //应用ID
        final String appId;
        //缓冲区
        final Iterator<ManagedBuffer> buffers;
        // The channel associated to the stream
        //关联通道
        Channel associatedChannel = null;
        // Used to keep track of the index of the buffer that the user
        // has retrieved, just to ensure
        // that the caller only requests each chunk one at a time, in order.
        //用于追踪用户检索的缓冲块的索引，只是为了确保调用者同一时间顺序的请求每个块。
        int curChunk = 0;
        StreamState(String appId, Iterator<ManagedBuffer> buffers) {
            this.appId = appId;
            this.buffers = Preconditions.checkNotNull(buffers);
        }

    }
    public OneForOneStreamManager() {
        // For debugging purposes, startServer with a random stream id to help identifying different streams.
        // This does not need to be globally unique, only unique to this class.

       /*
            为了调试目的，从随机流开始帮助辨别不同的流，。
            这个没有必要时全局唯一的，只在当前类唯一就可以了
        */

        nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
        streams = new ConcurrentHashMap<Long, StreamState>();
    }


    /*
    注册通道
     */
    @Override
    public void registerChannel(Channel channel, long streamId) {
        //如果包含流ID，就和这个管道关联上。
        if (streams.containsKey(streamId)) {
            streams.get(streamId).associatedChannel = channel;
        }
    }
    //获取块
    @Override
    public com.ning.buffer.ManagedBuffer getChunk(long streamId, int chunkIndex) {
        //获取流的状态
        StreamState state = streams.get(streamId);

        //如果请求的块索引和当前记录的块索引不相等
        if (chunkIndex != state.curChunk) {
            throw new IllegalStateException(String.format(
                    "Received out-of-order chunk index %s (expected %s)",
                    chunkIndex, state.curChunk));
        }else if (!state.buffers.hasNext()) {//如果没有下一个块内容  也报错
            throw new IllegalStateException(String.format(
                    "Requested chunk index beyond end %s", chunkIndex));
        }
        //服务端块索引 +1
        state.curChunk += 1;
        ManagedBuffer nextChunk = state.buffers.next();
        if (!state.buffers.hasNext()) {//已经发送所有的块  移除状态
            logger.trace("Removing stream id {}", streamId);
            streams.remove(streamId);
        }
        return nextChunk;
    }

    @Override
    public ManagedBuffer openStream(String streamId) {
        return super.openStream(streamId);
    }

    /**
     * 连接终止
     * @param channel
     */
    @Override
    public void connectionTerminated(Channel channel) {
        // Close all streams which have been associated with the channel.
        //关闭和该通道关联的所有状态流
        for (Map.Entry<Long, StreamState> entry: streams.entrySet()) {
            StreamState state = entry.getValue();
            if (state.associatedChannel == channel) {
                streams.remove(entry.getKey());

                // Release all remaining buffers.
                //每个状态流都释放所有的缓冲内容
                while (state.buffers.hasNext()) {
                    state.buffers.next().release();
                }
            }
        }
    }

    /**
     * 授权客户端
     * @param client
     * @param streamId
     */
    @Override
    public void checkAuthorization(TransportClient client, long streamId) {
        //如果客户端ID不为空
        if (client.getClientId() != null) {
            //获取流状态
            StreamState state = streams.get(streamId);
            //判断流状态
            Preconditions.checkArgument(state != null, "Unknown stream ID.");
            //如果客户端ID和状态流的APPID 不一样
            if (!client.getClientId().equals(state.appId)) {
                throw new SecurityException(String.format(
                        "Client %s not authorized to read stream %d (app %s).",
                        client.getClientId(),
                        streamId,
                        state.appId));
            }

        }
    }

    /**
     * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
     * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
     * client connection is closed before the iterator is fully drained, then the remaining buffers
     * will all be release()'d.
     *
     * If an app ID is provided, only callers who've authenticated with the given app ID will be
     * allowed to fetch from this stream.
     *
     *注册流
     */
    public long registerStream(String appId, Iterator<ManagedBuffer> buffers) {
        //生成流ID
        long myStreamId = nextStreamId.getAndIncrement();
        //注册流ID和流状态
        streams.put(myStreamId, new StreamState(appId, buffers));

        //返回流ID
        return myStreamId;
    }

}
