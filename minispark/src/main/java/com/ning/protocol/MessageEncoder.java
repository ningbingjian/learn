package com.ning.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *无状态的消息编码  线程安全
 */
public final  class MessageEncoder extends MessageToMessageEncoder<Message>{
    private final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);
    /**
     *通过执行消息本身的encode方法进行编码
     * 对于non-data[是指非二进制消息吗？],将会写出一个ByteBuf
     *ByteBuf包含总的帧长度，消息类型和消息内容本身。
     * 例如ChunkFetchSuccess 我们将写出与消息对应的ByteBuf ,
     * 确保0拷贝传输
     *
     * @param ctx
     * @param msg
     * @param out
     * @throws Exception
     */

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
        //消息内容
        Object body = null;
        //内容长度
        long bodyLength = 0;
        //内容是否包含在帧中
        boolean isBodyInFrame = false;
        if (msg.body() != null) {
            try{
                bodyLength = msg.body().size();
                body = msg.body().convertToNetty();
                isBodyInFrame = msg.isBodyInFrame();
            }catch(Exception e ){
                //如果是处理响应消息出错
                if (msg instanceof AbstractResponseMessage) {
                    AbstractResponseMessage resp = (AbstractResponseMessage) msg;
                    // Re-encode this message as a failure response.

                    String error = e.getMessage() != null ? e.getMessage() : "null";

                    logger.error(String.format("Error processing %s for client %s",
                            msg, ctx.channel().remoteAddress()), e);
                    //构造处理失败消息返回给客户端
                    encode(ctx, resp.createFailureResponse(error), out);
                } else {
                    throw e;
                }
                return;
            }
        }
        Message.Type msgType = msg.type();
        //设置头部长度 8个字节 + 消息类型的字节长度 + 消息内容体的长度
        //8个字节是描述本身的  因为headerLength是int类型 写入也占用了8个字节
        int headerLength =  8 + msgType.encodedLength() + msg.encodedLength();
        //整个消息帧的长度 等于 头部长度 + 内容长度 【如果消息内容也和发送帧一起，那么框架长度
        // 就是头部长度 + 消息内容体长度
        //否则那就只有头部内容体长度
        // 】
        long frameLength = headerLength + (isBodyInFrame ? bodyLength : 0);
        //分配头部长度的缓存
        ByteBuf header  = ctx.alloc().heapBuffer(headerLength);
        //写入整体发送帧的长度
        header.writeLong(frameLength);
        //写入消息类型
        msgType.encode(header);
        //内容写入
        msg.encode(header);
        assert header.writableBytes() == 0;
        if(body != null && bodyLength > 0 ){ //大部分消息都是这种方式 头和内容体一起发送
            out.add(new MessageWithHeader(header,body,bodyLength));
        }else{//流传输时这种方式  先描述流长度，后续会跟着具体的流内容 客户端需要具备解析后续流的能力a
            out.add(header);
        }

    }
}
