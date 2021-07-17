/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.storage.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RetryAgainException;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.google.protobuf.Message;

/**
 * Read a file data form local dir by fileName.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-06 9:25:12 PM
 */
public class LocalDirReader implements FileReader {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDirReader.class);

    private final String        path;
    protected final long        sliceSize;

    public LocalDirReader(String path, long sliceSize) {
        super();
        this.path = path;
        this.sliceSize = sliceSize;
    }

    @Override
    public long getSliceSize() {
        return sliceSize;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public int readFile(final ByteBufferCollector buf, final String fileName, final long sliceId, final long offset,
                        final long maxCount) throws IOException, RetryAgainException {
        return readFileWithMeta(buf, fileName, sliceId, null, offset, maxCount);
    }

    @SuppressWarnings("unused")
    protected int readFileWithMeta(final ByteBufferCollector buf, final String fileName, final long sliceId,
                                   final Message fileMeta, long offset, final long maxCount) throws IOException {
        buf.expandIfNecessary();
        final String filePath = this.path + File.separator + fileName;
        final File file = new File(filePath);
        try (final FileInputStream input = new FileInputStream(file); final FileChannel fc = input.getChannel()) {
            int totalRead = 0;
            final int nread = fc.read(buf.getBuffer(), sliceId * sliceSize + offset);
            if (nread <= 0) {
                return EOF;
            }
            buf.getBuffer().position(nread);
            totalRead += nread;
            if (totalRead == sliceSize - offset) {
                return EOF;
            } else {
                //Last slice
                if (sliceId * sliceSize + nread == file.length()) {
                    return EOF;
                }
                return totalRead;
            }
        }
    }
}
