package com.ning.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * Request to stream data from the remote end.
 * <p>
 * The stream ID is an arbitrary string that needs to be negotiated
 * between the two endpoints before
 * the data can be streamed.
 *请求从远端传输数据。
 *stream Id是在传输之前双方协商的一个任意的字符串
 *
 */
public class StreamRequest extends AbstractMessage implements RequestMessage {
    public final String streamId;
    public StreamRequest(String streamId){
        this.streamId = streamId;
    }
    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(streamId);
    }

    @Override
    public Type type() {
        return Type.StreamRequest;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf,streamId);
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof StreamRequest){
            StreamRequest o = (StreamRequest) other;
            return streamId.equals(o.streamId);
        }
        return false;
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(streamId);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamId",streamId)
                .toString();
    }

}
