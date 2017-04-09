/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Based on LimitedInputStream.java from Google Guava
 *
 * Copyright (C) 2007 The Guava Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.ning.util;

import com.google.common.base.Preconditions;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps a {@link InputStream}, limiting the number of bytes which can be read.
 *
 * This code is from Guava's 14.0 source code, because there is no compatible way to
 * use this functionality in both a Guava 11 environment and a Guava &gt;14 environment.

 InputStream的一个封装，限制流的读取字节数
 这个类的代码来自Guava 14的源码，因为没有可以在Guava11和Guava14两个版本兼容使用这个功能的方法。
 所以直接拷贝到项目来。



 */
public final class LimitedInputStream extends FilterInputStream {
  private long left;//初始化是limit 表示还剩多少字节没有读取完
  private long mark = -1;

  /**
   *
   * @param in  原始流
   * @param limit 可都去的大小限制
   */
  public LimitedInputStream(InputStream in, long limit) {
    super(in);
    Preconditions.checkNotNull(in);
    Preconditions.checkArgument(limit >= 0, "limit must be non-negative");
    left = limit;
  }
  //可读取字节数
  @Override
  public int available() throws IOException {
    return (int) Math.min(in.available(), left);
  }
  // it's okay to mark even if mark isn't supported, as reset won't work

  /**
   *标记当前位置
   * @param readLimit
   */
  @Override public synchronized void mark(int readLimit) {
    in.mark(readLimit);
    mark = left;
  }

  /**
   *读取下一个字节
   * @return
   * @throws IOException
   */
  @Override
  public int read() throws IOException {
    //已经读取了所有的字节
    if (left == 0) {
      return -1;
    }
    //读取下一个字节
    int result = in.read();
    if (result != -1) {
      --left;
    }
    return result;
  }

  /**
   *
   * @param b  保存内容的字节数组
   * @param off 从b的哪个位置开始保存
   * @param len 要读取的长度 和left比较 取最小的
   * @return
   * @throws IOException
   */
  @Override public int read(byte[] b, int off, int len) throws IOException {
    if (left == 0) {
      return -1;
    }
    len = (int) Math.min(len, left);
    int result = in.read(b, off, len);
    if (result != -1) {
      left -= result;
    }
    return result;
  }

  /**
   * 重置
   * @throws IOException
   */
  @Override
  public synchronized void reset() throws IOException {
    //检查是否支持标记
    if (!in.markSupported()) {
      throw new IOException("Mark not supported");
    }
    //如果没有设置过mark 就不能重置
    if (mark == -1) {
      throw new IOException("Mark not set");
    }
    //开始重置
    in.reset();
    //left 还是表示可以读取多少字节
    left = mark;
  }
  //跳过读取的字节数
  @Override public long skip(long n) throws IOException {
    n = Math.min(n, left);
    long skipped = in.skip(n);
    left -= skipped;
    return skipped;
  }
}
