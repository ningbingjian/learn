package com.ning.buffer;

import com.google.common.io.ByteStreams;
import com.ning.util.JavaUtils;
import com.ning.util.LimitedInputStream;
import com.ning.util.TransportConf;
import io.netty.channel.DefaultFileRegion;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A {@link ManagedBuffer} backed by a segment in a file.
 *支持文件的分段描述
 */
public class FileSegmentManagedBuffer extends  ManagedBuffer{
    private final TransportConf conf;
    private final File file;
    private final long offset;
    private final long length;

    public FileSegmentManagedBuffer(TransportConf conf, File file, long offset, long length) {
        this.conf = conf;
        this.file = file;
        this.offset = offset;
        this.length = length;
    }

    //分段文件大小
    @Override
    public long size() {
        return length;
    }

    //转换为nio 的ByteBuffer
    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        FileChannel channel = null;
        try {
            //读模式返回channel
            channel = new RandomAccessFile(file,"w").getChannel();
            if(length < conf.memoryMapBytes()){
                ByteBuffer buf = ByteBuffer.allocate((int)length);
                channel.position(offset);
                while(buf.hasRemaining()){
                    //读取到内存
                    if (channel.read(buf) == -1) {
                        throw new IOException(String.format("Reached EOF before filling buffer\n" +
                                        "offset=%s\nfile=%s\nbuf.remaining=%s",
                                offset, file.getAbsoluteFile(), buf.remaining()));
                    }
                }
                //进入读模式
                buf.flip();
                return buf;
            }else{
                return channel.map(FileChannel.MapMode.READ_ONLY,offset,length);
            }
        } catch (IOException e) {
            try {
                if (channel != null) {
                    long size = channel.size();
                    throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
                            e);
                }
            } catch (IOException ignored) {
                // ignore
            }
            throw new IOException("Error in opening " + this, e);
        }finally {
            JavaUtils.closeQuietly(channel);
        }
    }

    /**
     * 创建文件段的输入流
     * @return
     * @throws IOException
     */
    @Override
    public InputStream createInputStream() throws IOException {
        FileInputStream is = null;
        try{
            is = new FileInputStream(file);
            //直接跳到当前分段的在文件的偏移位置读取
            ByteStreams.skipFully(is, offset);
            return new LimitedInputStream(is,length);
        }catch (IOException e) {
            try {
                if (is != null) {
                    long size = file.length();
                    throw new IOException("Error in reading " + this + " (actual file length " + size + ")",
                            e);
                }
            } catch (IOException ignored) {
                // ignore
            } finally {
                JavaUtils.closeQuietly(is);
            }
            throw new IOException("Error in opening " + this, e);
        }catch (RuntimeException e) {
            JavaUtils.closeQuietly(is);
            throw e;
        }
    }


    public long getOffset() { return offset; }

    public long getLength() { return length; }
    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        if(conf.lazyFileDescriptor()){
            return new LazyFileRegion(file,offset,length);
        }else{
            FileChannel fileChannel = new FileInputStream(file).getChannel();
            return new DefaultFileRegion(fileChannel, offset, length);
        }
    }
}
