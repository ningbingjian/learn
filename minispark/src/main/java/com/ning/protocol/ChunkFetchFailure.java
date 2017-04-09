package com.ning.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;


/**
 * Response to {@link ChunkFetchRequest} when
 * there is an error fetching the chunk.
 * 响应ChunkFetchRequest,当在获取块的时候出错就会响应ChunkFetchFailure
 */
public class ChunkFetchFailure extends AbstractMessage implements ResponseMessage{
    public final StreamChunkId streamChunkId;
    public final String errorString;
    public ChunkFetchFailure(StreamChunkId streamChunkId,String errorString){
        this.streamChunkId = streamChunkId;
        this.errorString = errorString;
    }
    @Override
    public Type type() {
        return Type.ChunkFetchFailure;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength() + Encoders.Strings.encodedLength(errorString);
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
        Encoders.Strings.encode(buf,errorString);
    }
    public static ChunkFetchFailure decode(ByteBuf buf){
        StreamChunkId streamChunkId = StreamChunkId.decode(buf);
        String errorString = Encoders.Strings.decode(buf);
        return new ChunkFetchFailure(streamChunkId,errorString);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamChunkId,errorString);
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof ChunkFetchFailure){
            return streamChunkId.equals(other) && errorString.equals(((ChunkFetchFailure) other).errorString);
        }
        return false;
    }

}
