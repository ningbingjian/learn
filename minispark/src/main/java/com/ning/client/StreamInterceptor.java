package com.ning.client;

import com.ning.util.TransportFrameDecoder;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * An interceptor that is registered with the frame
 * decoder to feed stream data to a
 * callback.
 * 一个注册到帧解码器的拦截器，把流数据都反馈给回调函数
 */
public class StreamInterceptor implements TransportFrameDecoder.Interceptor {
    private final TransportResponseHandler handler;
    private final String streamId;
    private final long byteCount;
    private final StreamCallback callback;
    private volatile long bytesRead;
    StreamInterceptor(
            TransportResponseHandler handler,
            String streamId,
            long byteCount,
            StreamCallback callback) {
        this.handler = handler;//处理器
        this.streamId = streamId;//streamId
        this.byteCount = byteCount;//流的内容长度
        this.callback = callback;//回调
        this.bytesRead = 0;//已经读取字节数
    }
    @Override
    public boolean handle(ByteBuf buf) throws Exception {
        //计算可以读取的字节数
        int toRead = (int) Math.min(buf.readableBytes(), byteCount - bytesRead);
        //读取整个字节分片
        ByteBuffer nioBuffer = buf.readSlice(toRead).nioBuffer();
        //剩余字节数
        int available = nioBuffer.remaining();
        //回调函数处理流数据
        callback.onData(streamId, nioBuffer);
        //读取数据增加
        bytesRead += available;
        if (bytesRead > byteCount) {//读多了  不可以
            RuntimeException re = new IllegalStateException(String.format(
                    "Read too many bytes? Expected %d, but read %d.", byteCount, bytesRead));
            callback.onFailure(streamId, re);//作为失败处理
            handler.deactivateStream();//不激活流标志
        }else if (bytesRead == byteCount) {//刚好处理
            handler.deactivateStream();
            callback.onComplete(streamId);
        }

        return false;
    }

    @Override
    public void exceptionCaught(Throwable cause) throws Exception {
        handler.deactivateStream();
        callback.onFailure(streamId, cause);
    }

    @Override
    public void channelInactive() throws Exception {
        handler.deactivateStream();
        callback.onFailure(streamId, new ClosedChannelException());
    }
}
