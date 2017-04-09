package com.ning.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 此接口提供了字节形式的数据的不可变视图，实现应该指定如何提供数据
 *  {@link FileSegmentManagedBuffer}: 文件分片部分数据
 * - {@link NioManagedBuffer}: NIO ByteBuffer提供的数据
 * - {@link NettyManagedBuffer}: Netty中ByteBuf提供的数据
 */
public abstract class ManagedBuffer {
    /** 字节数据的长度 */
    public abstract long size();

    /**
     * 将缓冲区的数据暴露为NIO ByteBuffer .
     * 对返回的ByteBuffer改变位置和limit，
     * 不应该影响原来缓冲的内容
     */

    // TODO: Deprecate this,
   //usage may require expensive memory mapping or allocation.
    public abstract ByteBuffer nioByteBuffer() throws IOException;

    /**
     * 以InputStream的形式暴露缓冲区的数据、底层实现没有必要检查字节读取的长度，所以调用者有责任确保
     * 读取的内容没有超出limit
     */
    public abstract InputStream createInputStream() throws IOException;

    /**
     * 如果可以，调用这个方法的时候，将引用计数加1。
     */
    public abstract ManagedBuffer retain();

    /**
     * If applicable, decrement the reference count by one and deallocates the buffer if the
     * reference count reaches zero.
     * 如果可以，将引用计数减1，如果参考计数达到0 则释放缓冲区
     */
    public abstract ManagedBuffer release();

    /**
     * 将缓冲区对象转换为netty对象，用于写出数据
     */
    public abstract Object convertToNetty() throws IOException;
}
