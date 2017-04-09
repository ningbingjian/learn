package com.ning.util;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.LinkedList;


/**
 * A customized frame decoder that allows intercepting raw data.
 * <p>
 * This behaves like Netty's frame decoder (with harcoded parameters
 * that match this library's
 * needs), except it allows an interceptor to be installed to read data directly before it's
 * framed.
 * <p>
 * Unlike Netty's frame decoder, each frame is dispatched to child handlers as soon as it's
 * decoded, instead of building as many frames as the current buffer allows and dispatching
 * all of them. This allows a child handler to install an interceptor if needed.
 * <p>
 * If an interceptor is installed, framing stops, and data is instead fed directly to the
 * interceptor. When the interceptor indicates that it doesn't need to read any more data,
 * framing resumes. Interceptors should not hold references to the data buffers provided
 * to their handle() method.
 * 允许拦截原始数据的解码框架。
 * 和netty的框架解码器的行为类似[具有与类库需要匹配的硬编码参数]，
 * 还可以允许设置一个拦截器，可以在构建之前直接读取数据。
 * 和netty的帧解码器有所不同，每一个帧都在解码后发送给对应的子handler处理
 * 为了代替给当前的缓冲内容构建更多的帧解码器，然后分发解码内容这种做法，
 * 这个解码器允许当前的handler安装一个拦截器，如果有必要的话。
 * 如果一个拦截器被安装，构建过程就会停止。然后数据直接传送给拦截器。
 * 当拦截器表明它不在需要读取更多的数据时，框架就自动恢复。
 * 拦截器不应该持有通过handler方法传递给过来的缓冲buffer内容的引用
 *
 *
 *
 *
 *
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {
    //解码器的名称
    public static final String HANDLER_NAME = "frameDecoder";
    //整个框架的长度表示  8个字节 -->Long
    private static final int LENGTH_SIZE = 8;
    //最大的框架长度 整数的最大
    private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
    //未知框架长度
    private static final int UNKNOWN_FRAME_SIZE = -1;

    private final LinkedList<ByteBuf> buffers = new LinkedList<>();

    //帧长度缓冲 初始化缓冲区大小  保存框架长度的缓冲
    private final ByteBuf frameLenBuf = Unpooled.buffer(LENGTH_SIZE, LENGTH_SIZE);

    //记录总共大小
    private long totalSize = 0;
    //
    private long nextFrameSize = UNKNOWN_FRAME_SIZE;
    //框架的拦截器
    private volatile Interceptor interceptor;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
        //读取内容
        ByteBuf in = (ByteBuf) data;
        buffers.add(in);
        totalSize += in.readableBytes();
        while (!buffers.isEmpty()) {//只要列表的缓冲没有处理完 就一直处理
            if (interceptor != null) {//如果设置了拦截器
                ByteBuf first = buffers.getFirst();//每次先处理第一个
                //可读取字节数
                int available = first.readableBytes();
                if (feedInterceptor(first)) {
                    //拦截器还在激活状态 但是缓冲区没有可读取内容  这是不允许的
                    assert !first.isReadable() : "Interceptor still active but buffer has data.";
                }
                //计算剩余多少字节没有读取
                int read = available - first.readableBytes();
                if (read == available) {//如果可读取内容和读取的内容长度一致  那么就可以释放列表的缓冲
                    buffers.removeFirst().release();
                }
                //计算剩余读取内容
                totalSize -= read;
            }else{
                // Interceptor is not active, so try to decode one frame.
                //没有激活任何拦截器  直接解码框架
                ByteBuf frame = decodeNext();
                if (frame == null) {
                    break;
                }
                //继续往下调用 如果还有handler的话
                ctx.fireChannelRead(frame);
            }
        }
    }

    /**
     * @return Whether the interceptor is still active after processing the data.
     * 如果拦截器在处理完数据还处于激活状态 表示还需要继续读取数据
     */
    private boolean feedInterceptor(ByteBuf buf) throws Exception {
        //拦截器处理缓冲内容
        if (interceptor != null && !interceptor.handle(buf)) {
            interceptor = null;
        }
        return interceptor != null;
    }
    //解码下个帧应读的内容
    private ByteBuf decodeNext() throws Exception {
        //解码帧大小
        long frameSize = decodeFrameSize();

        if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
            return null;
        }
        // Reset size for next frame.
        //重置大小 留取下一个判断使用
        nextFrameSize = UNKNOWN_FRAME_SIZE;
       //帧大小必须小于最大限制
        Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE,
                "Too large frame: %s", frameSize);
       //帧长度必须是正数
        Preconditions.checkArgument(frameSize > 0,
                "Frame length should be positive: %s", frameSize);
        // If the first buffer holds the entire frame, return it.
        //如果第一缓冲就已经包含有额外所有的帧长度了  返回它
        //remaining表示具体内容的占用缓冲大小字节数
        int remaining = (int) frameSize;
        //如果缓冲区还有可读的内容 【在解析完帧的长度内容之后还有可读取内容，并且包含所有的数据，那么直接读取返回即可】
        if (buffers.getFirst().readableBytes() >= remaining) {
            //直接返回下个帧长度，也就是所有内容一次就能读取完
            return nextBufferForFrame(remaining);
        }
        // Otherwise, create a composite buffer.
        //否则 就创建一个复合的缓冲区
        CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
        while (remaining > 0) {
            //读取下个内容
            ByteBuf next = nextBufferForFrame(remaining);
            //计算还剩多少内容需要读取
            remaining -= next.readableBytes();
            //往复合缓冲区添加内容
            frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
        }
        assert remaining == 0;
        return frame;

    }
    /**
     * Takes the first buffer in the internal list, and either adjust it to fit in the frame
     * (by taking a slice out of it) or remove it from the internal list.

        在内部列表中获取第一个缓冲区，并调整它的大小【通过从其中取一个切片】以便符合帧或者从内部列表删除

     */
    private ByteBuf nextBufferForFrame(int bytesToRead) {
        //内部缓冲列表 第一个
        ByteBuf buf = buffers.getFirst();
        ByteBuf frame;//帧
        //如果可读字节数大于给定的读取数据量
        if (buf.readableBytes() > bytesToRead) {
            //帧的内容就是给定内容
            frame = buf.retain().readSlice(bytesToRead);
            //下轮需要读取总大小需要减去已经读取
            totalSize -= bytesToRead;
        } else {
            //缓冲区内容不足 只是部分
            frame = buf;
            //移除缓冲第一个
            buffers.removeFirst();
            //设置下轮需要读取的大小  也就是还有多少内容没有传输完
            totalSize -= frame.readableBytes();
        }

        return frame;
    }
    //解码帧的整体大小
    private long decodeFrameSize() {
        //如果下一帧的大小不是未知，并且总大小小于最大值 直接返回下一帧的大小
        //第一次调用肯定为FALSE
        if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < LENGTH_SIZE) {
            return nextFrameSize;
        }
        // We know there's enough data. If the first buffer contains all the data, great. Otherwise,
        // hold the bytes for the frame length in a composite buffer until we have enough data to read
        // the frame size. Normally, it should be rare to need more than one buffer to read the frame
        // size.
        /*
            如果我们知道有足够的数据，如果第一个buffer包含了所有的数据，那就很棒了。
            否则，需要保持复合buffer的帧长度的字节，直接我们有足够的数据可以读取帧大小，通常，很好需要多个buffer来读取帧大小的。
         */
        ByteBuf first = buffers.getFirst();
        //如果第一个缓冲区的可读字节数大于等于表示帧长度大小的限制
        if (first.readableBytes() >= LENGTH_SIZE) {
            //下一帧的长度就等于
            //readlong表示整个内容的总程度 减去描述内容长度本身的占用长度就是剩下内容的长度
            nextFrameSize = first.readLong() - LENGTH_SIZE;
            if (!first.isReadable()) {//没有可读内容
                buffers.removeFirst().release();
            }
            return nextFrameSize;
        }
        //当保存的框架长度缓冲内容小于长度所占的字节数  这种情况其实应该比较少 毕竟只有8个字节
        //基本上传输一次就可以完整读取了
        while (frameLenBuf.readableBytes() < LENGTH_SIZE) {
            ByteBuf next = buffers.getFirst();
            //设置应该读取的字节数
            int toRead = Math.min(next.readableBytes(), LENGTH_SIZE - frameLenBuf.readableBytes());
            //写入
            frameLenBuf.writeBytes(next,toRead);
            //没有可读取的内容 需要移除 并释放缓冲区
            if (!next.isReadable()) {
                buffers.removeFirst().release();
            }
        }
        //下一个帧的长度
        nextFrameSize = frameLenBuf.readLong() - LENGTH_SIZE;
        //缓冲区总大小设置
        totalSize -= LENGTH_SIZE;
        //清理帧长度缓冲区
        frameLenBuf.clear();
        return nextFrameSize;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        for (ByteBuf b : buffers) {
            b.release();
        }
        if (interceptor != null) {
            interceptor.channelInactive();
        }
        frameLenBuf.release();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if(interceptor != null){
            interceptor.exceptionCaught(cause);
        }
        super.exceptionCaught(ctx,cause);
    }



    public void setInterceptor(Interceptor interceptor) {
        this.interceptor = interceptor;
    }

    //拦截器
    public static interface Interceptor {

        /**
         * Handles data received from the remote end.
         * 处理从远端接收的数据
         *
         * @param data Buffer containing data. 数据内容
         * @return "true" if the interceptor expects more data,
         * "false" to uninstall the interceptor.
         * 如果拦截器需要更多的内容就返回true
         * 如果需要卸载当前的拦截器，就返回false
         */
        boolean handle(ByteBuf data) throws Exception;

        /** Called if an exception is thrown in the channel pipeline.
         * 异常的回调
         * */
        void exceptionCaught(Throwable cause) throws Exception;

        /** Called if the channel is closed and the interceptor is still installed. */
        void channelInactive() throws Exception;

    }
}
