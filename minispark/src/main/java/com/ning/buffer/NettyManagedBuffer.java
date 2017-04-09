package com.ning.buffer;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by zhaoshufen on 2017/3/28.
 */
public class NettyManagedBuffer extends ManagedBuffer {
    private final ByteBuf buf;
    public NettyManagedBuffer(ByteBuf buf) {
        this.buf = buf;
    }
    @Override
    public long size() {
        //缓冲区大小
        return buf.readableBytes();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buf.nioBuffer();
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(buf);
    }
    //引用计数减1
    @Override
    public ManagedBuffer retain() {
        buf.release();
        return this;
    }

    //返回netty对象 共享内存块 修改会互相影响
    @Override
    public Object convertToNetty() throws IOException {
        return buf.duplicate();
    }

    @Override
    public ManagedBuffer release() {
        buf.release();
        return this;
    }

    //
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("buf", buf)
                .toString();
    }
}
