package com.ning.protocol;


import com.google.common.base.Objects;
import com.ning.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * Request to fetch a sequence of a single chunk of a stream. This will correspond to a single
 *请求获取单个流的单个块的序列 这个请求对应一个ResponseMessage
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 *
 * 注意：CHunkFetchRequest并没有实现 body() 因为body并没有和header一起发送
 */
public class ChunkFetchRequest  extends AbstractMessage implements RequestMessage {
    public final StreamChunkId streamChunkId;
    public ChunkFetchRequest(StreamChunkId streamChunkId){
        this.streamChunkId = streamChunkId;
    }
    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
    }

    @Override
    public int hashCode() {
        return streamChunkId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchRequest) {
            ChunkFetchRequest o = (ChunkFetchRequest) other;
            return streamChunkId.equals(o.streamChunkId);
        }
        return false;
    }
    @Override
    public Type type() {
        return Type.ChunkFetchRequest;
    }


    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamChunkId", streamChunkId)
                .toString();
    }
    public static ChunkFetchRequest decode(ByteBuf buf){
        return new ChunkFetchRequest(StreamChunkId.decode(buf));
    }
}
