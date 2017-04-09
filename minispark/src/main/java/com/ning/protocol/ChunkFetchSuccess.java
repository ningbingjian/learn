package com.ning.protocol;


import com.ning.buffer.ManagedBuffer;
import com.ning.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * Response to {@link ChunkFetchRequest} when a chunk exists and has been successfully fetched.
 *
 * Note that the server-side encoding of this messages does NOT include the buffer
 * itself, as this
 * may be written by Netty in a more efficient manner (i.e., zero-copy write).
 * Similarly, the client-side decoding will reuse the Netty ByteBuf as the buffer.
 对ChunkFetchRequest的响应 ，当一个块存在并且已经成功获取到

 注意  这个消息的服务端编码不包含缓冲区本身。因为netty可以以更有效的方式来写入[零拷贝写入]
 类似地，客户端解码将重用netty的byteBuf作为缓冲区


 */
public class ChunkFetchSuccess extends  AbstractResponseMessage{
    public final StreamChunkId streamChunkId;
    public ChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer) {
        super(buffer, true);
        this.streamChunkId = streamChunkId;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchSuccess;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return new ChunkFetchFailure(streamChunkId, error);
    }
    /** Decoding uses the given ByteBuf as our data, and will retain() it.
     * 从给定的ByteBuf反编码作为我们的数据。并且保留ByteBuf
     * */
    public static ChunkFetchSuccess decode(ByteBuf buf){
        StreamChunkId streamChunkId = StreamChunkId.decode(buf);
        buf.retain();
        //重用buf
        NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());

        return new ChunkFetchSuccess(streamChunkId,managedBuf);
    }
}
