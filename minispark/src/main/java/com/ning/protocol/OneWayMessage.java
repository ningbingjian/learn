package com.ning.protocol;

import com.google.common.base.Objects;
import com.ning.buffer.ManagedBuffer;
import com.ning.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;


/**
 * A RPC that does not expect a reply, which is handled by a remote
 * {@link org.apache.spark.network.server.RpcHandler}.
 * 一个不期望响应的RPC消息，直接由远端进行处理
 */
public class OneWayMessage extends AbstractMessage implements RequestMessage {

    public OneWayMessage(ManagedBuffer body) {
        super(body, true);
    }
    @Override
    public Type type() {
        return Type.OneWayMessage;
    }

    @Override
    public int encodedLength() {
        // The integer (a.k.a. the body size) is not really used, since that information is already
        // encoded in the frame length. But this maintains backwards compatibility with versions of
        // RpcRequest that use Encoders.ByteArrays.
        /**
         * 这个长度并没有真正的被使用，因为该信息已经被编码在帧长度中。但是这与使用Encoders.ByteArrays的RpcRequest
         * 版本可以保持向后兼容
         *
         */
        return 4;
    }
    public static OneWayMessage decode(ByteBuf buf){
        int size = buf.readInt();
        return new OneWayMessage(new NettyManagedBuffer(buf));
    }
    @Override
    public int hashCode() {
        return Objects.hashCode(body());
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof OneWayMessage) {
            OneWayMessage o = (OneWayMessage) other;
            return super.equals(o);
        }
        return false;
    }

    @Override
    public void encode(ByteBuf buf) {
        // See comment in encodedLength().
        buf.writeInt((int) body().size());
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("body", body())
                .toString();
    }
}
