package com.ning.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zhaoshufen on 2017/3/31.
 */
public class MessageDecoder extends MessageToMessageDecoder<ByteBuf>{
    private final Logger logger = LoggerFactory.getLogger(MessageDecoder.class);
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Message.Type msgType = Message.Type.decode(in);
        Message decoded = decode(msgType,in);
        assert decoded.type() == msgType;
        logger.trace("Received message " + msgType + ": " + decoded);
        out.add(decoded);
    }
    private Message decode(Message.Type msgType,ByteBuf buf){
        switch (msgType){
            case RpcRequest:
                return RpcRequest.decode(buf);
            case RpcResponse:
                return RpcResponse.decode(buf);
            default:
                throw new IllegalArgumentException("Unexpected message type: " + msgType);
        }
    }
}
