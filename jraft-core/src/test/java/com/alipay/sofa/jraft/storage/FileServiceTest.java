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
package com.alipay.sofa.jraft.storage;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.storage.io.LocalDirReader;
import com.alipay.sofa.jraft.test.TestUtils;
import com.google.protobuf.Message;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileServiceTest {

    private String         path;
    private LocalDirReader fileReader;

    @Before
    public void setup() throws Exception {
        this.path = TestUtils.mkTempDir();
        this.fileReader = new LocalDirReader(path, 1024);
    }

    @After
    public void teardown() {
        FileUtils.deleteQuietly(new File(this.path));
        FileService.getInstance().clear();
    }

    @Test
    public void testAddRemove() {
        long readerId = FileService.getInstance().addReader(this.fileReader);
        assertTrue(readerId > 0);
        assertTrue(FileService.getInstance().removeReader(readerId));
    }

    @Test
    public void testGetFileNotFoundReader() {
        RpcRequests.GetFileRequest request = RpcRequests.GetFileRequest.newBuilder().setCount(Integer.MAX_VALUE)
            .setFilename("data").setOffset(0).setReaderId(1).build();
        RpcContext asyncContext = Mockito.mock(RpcContext.class);
        Message msg = FileService.getInstance().handleGetFile(request, new RpcRequestClosure(asyncContext));
        assertTrue(msg instanceof RpcRequests.ErrorResponse);
        RpcRequests.ErrorResponse response = (RpcRequests.ErrorResponse) msg;
        Assert.assertEquals(RaftError.ENOENT.getNumber(), response.getErrorCode());
        assertEquals("Fail to find reader=1", response.getErrorMsg());
    }

    @Test
    public void testGetFileNotFound() {
        long readerId = FileService.getInstance().addReader(this.fileReader);
        RpcRequests.GetFileRequest request = RpcRequests.GetFileRequest.newBuilder().setCount(Integer.MAX_VALUE)
            .setFilename("data").setOffset(0).setReaderId(readerId).build();
        RpcContext asyncContext = Mockito.mock(RpcContext.class);
        Message msg = FileService.getInstance().handleGetFile(request, new RpcRequestClosure(asyncContext));
        assertTrue(msg instanceof RpcRequests.ErrorResponse);
        RpcRequests.ErrorResponse response = (RpcRequests.ErrorResponse) msg;
        assertEquals(RaftError.EIO.getNumber(), response.getErrorCode());
        assertEquals(String.format("Fail to read from path=%s filename=data", this.path), response.getErrorMsg());
    }

    private String writeData() throws IOException {
        File file = new File(this.path + File.separator + "data");
        String data = "jraft is great!";
        FileUtils.writeStringToFile(file, data);
        return data;
    }

    @Test
    public void testGetFileData() throws IOException {
        writeData();
        long readerId = FileService.getInstance().addReader(this.fileReader);
        RpcRequests.GetFileRequest request = RpcRequests.GetFileRequest.newBuilder().setCount(Integer.MAX_VALUE)
            .setFilename("data").setOffset(0).setReaderId(readerId).setSliceId(0).build();
        RpcContext asyncContext = Mockito.mock(RpcContext.class);
        Message msg = FileService.getInstance().handleGetFile(request, new RpcRequestClosure(asyncContext));
        assertTrue(msg instanceof RpcRequests.GetFileResponse);
        RpcRequests.GetFileResponse response = (RpcRequests.GetFileResponse) msg;
        assertTrue(response.getEof());
        assertEquals("jraft is great!", new String(response.getData().toByteArray()));
        assertEquals(-1, response.getReadSize());
    }

    private String writeLargeData() throws IOException {
        File file = new File(this.path + File.separator + "data");
        String data = "jraft is great!";
        for (int i = 0; i < 1000; i++) {
            FileUtils.writeStringToFile(file, data, true);
        }
        return data;
    }

    @Test
    public void testGetLargeFileData() throws IOException {
        final String data = writeLargeData();
        final long readerId = FileService.getInstance().addReader(this.fileReader);
        int fileOffset = 0;
        while (true) {
            final RpcRequests.GetFileRequest request = RpcRequests.GetFileRequest.newBuilder() //
                .setCount(4096).setFilename("data") //
                .setOffset(fileOffset) //
                .setReaderId(readerId) //
                .build();
            final RpcContext asyncContext = Mockito.mock(RpcContext.class);
            final Message msg = FileService.getInstance() //
                .handleGetFile(request, new RpcRequestClosure(asyncContext));
            assertTrue(msg instanceof RpcRequests.GetFileResponse);
            final RpcRequests.GetFileResponse response = (RpcRequests.GetFileResponse) msg;
            final byte[] sourceArray = data.getBytes();
            final byte[] respData = response.getData().toByteArray();
            final int length = sourceArray.length;
            int offset = 0;
            while (offset + length <= respData.length) {
                final byte[] respArray = new byte[length];
                System.arraycopy(respData, offset, respArray, 0, length);
                assertArrayEquals(sourceArray, respArray);
                offset += length;
            }
            fileOffset += offset;
            if (response.getEof()) {
                break;
            }
        }
    }
}
