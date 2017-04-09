package com.ning.protocol;

import com.ning.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * Created by zhaoshufen on 2017/3/27.
 */
public interface Message  extends Encodable{
    /** 返回请求类型 */
    Type type();

    /** 可选的消息主体 */
    ManagedBuffer body();

    /** 是否把消息的正文包含在与消息相同的帧中 */

    boolean isBodyInFrame();

    public static enum Type implements Encodable {
        ChunkFetchRequest(0), //数据块请求
        ChunkFetchSuccess(1),//数据块请求成功
        ChunkFetchFailure(2),//数据块请求失败
        RpcRequest(3),//Rpc请求
        RpcResponse(4),//Rpc相应
        RpcFailure(5),//rpc请求失败
        StreamRequest(6),//数据流请求
        StreamResponse(7),//数据流响应
        StreamFailure(8),//数据流请求失败
        OneWayMessage(9),//单向消息
        User(-1);//未知?

        private final byte id;
        private Type(int id){
            assert id < 128 : "Cannot have more than 128 message types";
            this.id = (byte)id;
        }

        public byte getId() {
            return id;
        }

        /**
         * 消息类型只占用一个字节
         * @return
         */
        @Override
        public int encodedLength() {
            return 1;
        }

        /**
         * 序列化当前消息类型
         * @param buf
         */
        @Override
        public void encode(ByteBuf buf) {
            buf.writeInt(id);
        }

        public static Message.Type decode(ByteBuf buf){
            byte id = buf.readByte();
            switch (id){
                case 0: return ChunkFetchRequest;
                case 1: return ChunkFetchSuccess;
                case 2: return ChunkFetchFailure;
                case 3: return RpcRequest;
                case 4: return RpcResponse;
                case 5: return RpcFailure;
                case 6: return StreamRequest;
                case 7: return StreamResponse;
                case 8: return StreamFailure;
                case 9: return OneWayMessage;
                case -1: throw new IllegalArgumentException("User type messages cannot be decoded.");
                default: throw new IllegalArgumentException("Unknown message type: " + id);
            }
        }
    }
}
