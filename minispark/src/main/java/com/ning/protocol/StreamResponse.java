package com.ning.protocol;

import com.google.common.base.Objects;
import com.ning.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;
/**
 * Response to {@link StreamRequest} when the stream has been successfully opened.
 * <p>
 * Note the message itself does not contain the stream data.
 * That is written separately by the
 * sender. The receiver is expected to set a temporary channel handler that will consume the
 * number of bytes this message says the stream has.
 StreamRequest的响应消息。
 当传输流能成功打开的时候响应StreamRequest消息
注意消息本身并没有携带任何流的数据。
流的数据内容是由发送方分开写出的。
 接收方预期设置一个临时的管道处理器来消费发送方所描述的发送字节数。
也就是发送方发送多少，接收方就设置处理多少数据。

 */
public class StreamResponse extends AbstractResponseMessage {
    public final String streamId;
    //发送方将要发送的数据大小
    public final long byteCount;
    public StreamResponse(String streamId, long byteCount, ManagedBuffer buffer) {
        super(buffer, false);
        this.streamId = streamId;
        this.byteCount = byteCount;
    }

    @Override
    public Type type() {
        return Type.StreamResponse;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(streamId) + 8;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf,streamId);
        buf.writeLong(byteCount);
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId,byteCount);
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof StreamResponse){
            StreamResponse o = (StreamResponse)other;
            return streamId.equals(o.streamId) && byteCount == o.byteCount;
        }
        return false;
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamId", streamId)
                .add("byteCount", byteCount)
                .add("body", body())
                .toString();
    }
}
