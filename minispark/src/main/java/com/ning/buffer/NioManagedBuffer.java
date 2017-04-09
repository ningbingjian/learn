package com.ning.buffer;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 由底层nio实现的ManagedBuffer
 */
public class NioManagedBuffer extends ManagedBuffer {
    private final ByteBuffer buf;
    public NioManagedBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public long size() {
        return buf.remaining();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buf.duplicate();
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(Unpooled.wrappedBuffer(buf));
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        return Unpooled.wrappedBuffer(buf);
    }
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("buf", buf)
                .toString();
    }
}
