package com.ning.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * Encapsulates a request for a particular chunk of a stream.
 * 封装一个流的特定块请求
 *
 */
public class StreamChunkId implements Encodable{
    //整个流的唯一ID streamId
    public final long streamId;
    //某个流的某一个chunk的索引  一个流由多个chunk组成
    public final int chunkIndex;
    public StreamChunkId(long streamId, int chunkIndex) {
        this.streamId = streamId;
        this.chunkIndex = chunkIndex;
    }

    /**
     * streamingId 8个字节
     * chunkIndex 4个字节
     * @return
     */
    @Override
    public int encodedLength() {
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(streamId);
        buf.writeInt(chunkIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId,chunkIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamChunkId that = (StreamChunkId) o;

        if (streamId != that.streamId) return false;
        return chunkIndex == that.chunkIndex;
    }
    public static StreamChunkId decode(ByteBuf buf){
        assert buf.readableBytes() > 8 + 4 ;
        long streamId = buf.readLong();
        int chunkIndex = buf.readInt();
        return new StreamChunkId(streamId,chunkIndex);
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamingId",streamId)
                .add("chunkIndex",chunkIndex)
                .toString();
    }


}
