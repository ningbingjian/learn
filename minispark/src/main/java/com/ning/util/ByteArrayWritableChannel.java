package com.ning.util;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A writable channel that stores the written data in a byte array in memory.
  一个可写的通道，可以将写入的数据存储在内存中的字节数组


 */
public class ByteArrayWritableChannel implements WritableByteChannel{
    private final byte[] data;
    private int offset;

    public ByteArrayWritableChannel(int size){
        data = new byte[size];
    }

    public byte[] getData() {
        return data;
    }

    public int length() {
        return offset;
    }
    /** Resets the channel so that writing to it will
     *  overwrite the existing buffer.
     *  重置通道，以便写入它的数据可以覆盖原来存在的缓冲区内容
     *
     *  */

    public void reset() {
        offset = 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int toTransfer = Math.min(src.remaining(),data.length - offset);
        src.get(data,offset,toTransfer);
        offset += toTransfer ;
        return toTransfer;
    }
}
