package com.ning.protocol;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 A wrapper message that holds two separate pieces (a header and a body).
 *一个包含两个独立部分的包装消息[头部和内容体]
 * The header must be a ByteBuf, while the body can be a ByteBuf or a FileRegion.
 * 头部必须是ByteBuf类型,而body可以是一个ByteBuf或者FileRegion类型
 */
public class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuf header;
    private final int headerLength;
    private final Object body;
    private final long bodyLength;
    private long totalBytesTransferred;

    /**
     * When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
     * The size should not be too large as it will waste underlying memory copy. e.g. If network
     * available buffer is smaller than this limit, the data cannot be sent within one single write
     * operation while it still will make memory copy with this size.
     */
    /**
     *如果缓冲区的大小大于这个限制，IO会以这个大小的chunk[块]的形式完成
     * 这个数值不能太大，因为太大会浪费底层的内存拷贝
     * 如果网络可用缓冲小于这个值，数据就不能在一次单一的写操作中发送，同时仍然使用这个大小来做内存拷贝
     *
     */
    private static final int NIO_BUFFER_LIMIT = 256 * 1024;
    public MessageWithHeader(ByteBuf header,Object body, long bodyLength) {
        Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
                "Body must be a ByteBuf or a FileRegion.");
        this.header = header;
        this.body = body;
        this.headerLength = header.readableBytes();
        this.bodyLength = bodyLength;
    }

    /**
     * 返回传输的开始位置
     */
    public long position(){
        return 0 ;
    }

    /**
     * 返回已经传输的内容长度
     */
    public long transfered(){
        return totalBytesTransferred ;
    }

    /**
     * 返回传输字节的数量  头部长度和内容体的长度
     */
    public long count(){
        return headerLength + bodyLength;
    }

    /**
     * Transfers the content of this file region to the specified channel.
     *
     * @param target    the destination of the transfer
     * @param position  the relative offset of the file where the transfer
     *                  begins from.  For example, <tt>0</tt> will make the
     *                  transfer start from {@link #position()}th byte and
     *                  <tt>{@link #count()} - 1</tt> will make the last
     *                  byte of the region transferred.
     *
     *
     *传输内容
     *target :传输的目标
      position 位置 从文件的哪个位置开始传输。例如0 就会从开始位置传输
                        而count-1将会从region
     */
    public long transferTo(WritableByteChannel target, long position) throws IOException{
        Preconditions.checkArgument(position == totalBytesTransferred, "Invalid position.");
        // Bytes written for header in this call.
        long writtenHeader = 0;
        //如果头部还没传输完 继续传输
        if (header.readableBytes() > 0) {
            writtenHeader = copyByteBuf(header, target);
            totalBytesTransferred += writtenHeader;
            if (header.readableBytes() > 0) {
                return writtenHeader;
            }
        }

        // 内容体传输
        long writtenBody = 0;
        //如果是FileRegion类型
        if (body instanceof FileRegion) {
            //调用具体的FileRegion写出
            writtenBody = ((FileRegion) body).transferTo(target, totalBytesTransferred - headerLength);
        //如果是ByteBuf类型
        } else if (body instanceof ByteBuf) {
            //调用这里的copyByteBuf写出
            writtenBody = copyByteBuf((ByteBuf) body, target);
        }
        //设置总写出的字节数量
        totalBytesTransferred += writtenBody;
        //返回写出的头部长度和
        return writtenHeader + writtenBody;
    }
    private int copyByteBuf(ByteBuf buf, WritableByteChannel target) throws IOException {
        //转换为nio buf
        ByteBuffer buffer = buf.nioBuffer();
        //写入目标通道  如果剩下未写出的内容小于写出限制  直接写出全部 否则 调用writeNioBuffer  写出部分
        //剩下未写出的等待本次写完继续写
        int written = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                target.write(buffer) : writeNioBuffer(target, buffer);
        buf.skipBytes(written);
        return written;
    }
    //写出
    private int writeNioBuffer(
            WritableByteChannel writeCh,
            ByteBuffer buf) throws IOException {
        //缓冲区的limit
        int originalLimit = buf.limit();
        int ret = 0;

        try {
            //剩余大小和最大限制 取最小的
            int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
            //将limit设置到要写出的位置
            buf.limit(buf.position() + ioSize);
            //写出
            ret = writeCh.write(buf);
        } finally {
            //将原来的limit恢复到正确的位置
            buf.limit(originalLimit);
        }

        return ret;
    }

    @Override
    protected void deallocate() {
        header.release();
        ReferenceCountUtil.release(body);
    }
}
