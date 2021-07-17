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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.util.ByteBufferCollector;

public class LocalFileReaderTest extends BaseStorageTest {
    private LocalDirReader fileReader;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.fileReader = new LocalDirReader(path, 1024);
    }

    @Test
    public void testReadFile() throws Exception {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate();
        try {
            this.fileReader.readFile(bufRef, "unfound", 0, 0, 1024);
            fail();
        } catch (final FileNotFoundException e) {

        }

        final String data = writeData();

        assertReadResult(bufRef, data);
    }

    private void assertReadResult(ByteBufferCollector bufRef, String data) throws Exception {
        final int read = this.fileReader.readFile(bufRef, "data", 0, 0, 1024);
        assertEquals(-1, read);
        final ByteBuffer buf = bufRef.getBuffer();
        buf.flip();
        assertEquals(data.length(), buf.remaining());
        final byte[] bs = new byte[data.length()];
        buf.get(bs);
        assertEquals(data, new String(bs));
    }

    @Test
    public void testReadSmallInitBuffer() throws Exception {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(2);

        final String data = writeData();

        assertReadResult(bufRef, data);
    }

    @Test
    public void testReadBigFile() throws Exception {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(2);

        final File file = new File(this.path + File.separator + "data");
        String data = "";
        for (int i = 0; i < 4096; i++) {
            data += i % 10;
        }
        FileUtils.writeStringToFile(file, data);

        int read = this.fileReader.readFile(bufRef, "data", 0, 0, 1024);
        assertEquals(1024, read);
        read = this.fileReader.readFile(bufRef, "data", 0, 1024, 1024);
        assertEquals(1024, read);
        read = this.fileReader.readFile(bufRef, "data", 0, 1024 + 1024, 1024);
        assertEquals(1024, read);
        read = this.fileReader.readFile(bufRef, "data", 0, 1024 + 1024 + 1024, 1024);
        assertEquals(-1, read);

        final ByteBuffer buf = bufRef.getBuffer();
        buf.flip();
        assertEquals(data.length(), buf.remaining());
        final byte[] bs = new byte[data.length()];
        buf.get(bs);
        assertEquals(data, new String(bs));

    }
}
