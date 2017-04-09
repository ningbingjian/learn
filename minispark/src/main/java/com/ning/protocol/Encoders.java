package com.ning.protocol;

import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/** 为简单类型提供一套规范的编码器. */
public class Encoders {
    public static void main(String[] args) {
        Long l = 7L;
        ByteBuffer buf = ByteBuffer.allocate(100);
        buf.putLong(l);
        buf.flip();
        while(buf.hasRemaining()){
            System.out.print(buf.get());
        }

    }
    public static class Strings{
        //String编码长度计算
        public static int encodedLength(String s) {
            //4个字节  -->表示内容体的长度整数表示
            // String转为bytes之后的长度
            return 4 + s.getBytes(Charsets.UTF_8).length;
        }
        public static void encode(ByteBuf buf,String s){
            byte[]bytes = s.getBytes(Charsets.UTF_8);
            //先写整个消息的长度
            buf.writeInt(bytes.length);
            //再写消息内容
            buf.writeBytes(bytes);

        }
        public static String decode(ByteBuf buf){
            //读取消息长度 前四个字节
            int length = buf.readInt();
            //保存消息内容
            byte[]bytes = new byte[length];
            //读取内容
            buf.readBytes(bytes);
            //返回字符
            return new String(bytes,Charsets.UTF_8);
        }
    }
    /** Byte arrays are encoded
     * with their length followed by bytes.
     * 字节数组编码
     * 字节数组的编码是通过字节数组的长度和字节本身进行编码
     * */

    public static class ByteArrays{
        public static int encodedLength(byte[] arr){
           return 4 + arr.length;
        }
        public static void encode(ByteBuf buf,byte[] arr){
            buf.writeInt(arr.length);
            buf.writeBytes(arr);
        }
        public static byte[] decode(ByteBuf buf){
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return bytes;
        }
    }

    /** String arrays are encoded with the number
     * of strings followed by per-String encoding.
     * 字符串数组使用字符串数字编码，后跟每个字符串编码。
     * */

    public static class StringArrays{
        public static int encodedLength(String[] strings) {
            int totalLength = 4 ;//表示有多少个字符串,长度表示用int来标示  int 是4个字节
            for(String s : strings){//总长度就是所有字符串的编码长度累加
                totalLength += Strings.encodedLength(s);
            }
            return totalLength;
        }
        public static void encode(ByteBuf buf,String[] strings){
            buf.writeInt(strings.length);
            for(String s : strings){
                Strings.encode(buf,s);
            }
        }
        public static String[] decode(ByteBuf buf){
            int numString = buf.readInt();//读取第一个整数  有几个字符串数组
            String[] strings = new String[numString];
            for(int i = 0 ;i < strings.length; i ++){
                strings[i] = Strings.decode(buf);
            }
            return strings;

        }
    }
}
