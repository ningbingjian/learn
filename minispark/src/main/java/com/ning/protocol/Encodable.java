package com.ning.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Created by zhaoshufen on 2017/3/27.
 * 可以被编码成ByteBuf对象的接口，多个编码对象存储在一个预先分配的ByteBuf中
 * 所以Encodeable必须存储他们的长度，
 * 可编码的对象应该提供一个静态的decode(ByteBuf)方法
 * 在解码的过程中，如果对象使用ByteBuf作为其数据，而不是只是复制数据，那么你必须保留ByteBuf
 * 另外如果添加新的编码 ，应该把它添加到Message.Type类型中
 *
 */
public interface Encodable {
    /**
     *当前对象的编码所占字节数
     * @return
     */
    int encodedLength();

    /**
     *通过给定的ByteBuf序列化对象，这个方法必须编写正确的encodeLength()字节数
     * @param buf
     */
    void encode(ByteBuf buf);
}
