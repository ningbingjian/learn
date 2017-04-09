package com.ning.protocol;

import com.google.common.base.Objects;
import com.ning.buffer.ManagedBuffer;
import com.ning.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhaoshufen on 2017/3/28.
 */
public final class RpcRequest extends AbstractMessage implements RequestMessage {
    /** 用于和rpc相应相关联  */
    public final long requestId;
    public RpcRequest(long requestId, ManagedBuffer message) {
        //默认消息长度和消息体都在同一个帧内
        super(message, true);
        this.requestId = requestId;
    }
    @Override
    public Type type() { return Type.RpcRequest; }

    @Override
    public int encodedLength() {
        /**
         * 整数【消息体长度的数字】并没有使用
         * 因为消息体已经编码到同一个帧中。
         * 但是为了保留向后兼容那些使用Encoders.ByteArrays的请求消息
         * 8个字节 + 4个字节
         *
         *这里我还是不太理解
         * 请求IDrequestId--》Long类型 8个字节
         * 后面4个字节不太理解
         */

        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        //请求ID
        buf.writeLong(requestId);
        buf.writeInt((int) body().size());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(requestId,body());
    }

    @Override
    protected boolean equals(AbstractMessage other) {
        if (other instanceof RpcRequest) {
            RpcRequest o = (RpcRequest) other;
            return requestId == o.requestId && super.equals(o);
        }
        return false;
    }

    public static RpcRequest decode(ByteBuf buf) {
        long requestId = buf.readLong();
        //和 encodedLength()对比看
        buf.readInt();
        //反编码  构造成RpcRequest
        return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
    }
}
