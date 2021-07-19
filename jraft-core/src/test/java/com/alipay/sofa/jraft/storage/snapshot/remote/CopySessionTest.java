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
package com.alipay.sofa.jraft.storage.snapshot.remote;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CopyOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcUtils;
import com.alipay.sofa.jraft.rpc.impl.FutureImpl;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

@RunWith(value = MockitoJUnitRunner.class)
public class CopySessionTest {
    private CopySession                        session;
    @Mock
    private RaftClientService                  rpcService;
    private RpcRequests.GetFileRequest.Builder rb;
    private final Endpoint                     address = new Endpoint("localhost", 8081);
    private CopyOptions                        copyOpts;
    private RaftOptions                        raftOpts;
    private TimerManager                       timerManager;
    private String                             dataDir;

    @Before
    public void setup() throws IOException {
        this.timerManager = new TimerManager(5);
        this.copyOpts = new CopyOptions();
        this.rb = RpcRequests.GetFileRequest.newBuilder();
        this.rb.setReaderId(99);
        this.rb.setFilename("data");
        this.raftOpts = new RaftOptions();
        this.raftOpts.setMaxByteCountPerRpc(100);
        this.raftOpts.setDownloadingSnapshotConcurrency(1);
        this.session = new CopySession(rpcService, timerManager, null, raftOpts, rb, address);
        this.session.setCopyOptions(copyOpts);
        this.dataDir = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataDir));
    }

    @After
    public void teardown() throws IOException {
        Utils.closeQuietly(this.session);
        this.timerManager.shutdown();
        FileUtils.deleteDirectory(new File(this.dataDir));
    }

    @Test
    public void testSendNextRpc() throws IOException {
        final int maxCount = this.raftOpts.getMaxByteCountPerRpc();
        this.session.init(this.dataDir + File.separator + "data", maxCount, null);
        sendNextRpc(maxCount);
    }

    @Test
    public void testSendNextRpcWithBuffer() throws IOException {
        session.init(null, 0, ByteBufferCollector.allocate(1));
        final int maxCount = this.raftOpts.getMaxByteCountPerRpc();
        sendNextRpc(maxCount);
    }

    @Test
    public void testDownloadFinish() throws Exception {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.init(null, 0, bufRef);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            @Override
            public void run() {
                try {
                    //test join, should return
                    session.join();
                    latch.countDown();
                } catch (final InterruptedException e) {

                }
            }
        }.start();

        DownloadContext context = this.session.getDownloadManager().getDownloadContext(0);

        assertNull(context.getRpcCall());

        this.session.onRpcReturned(context, Status.OK(), RpcRequests.GetFileResponse.newBuilder().setReadSize(100)
            .setEof(true).setData(ByteString.copyFrom(new byte[100])).build());
        assertEquals(100, bufRef.capacity());
        //should be flip
        assertEquals(0, bufRef.getBuffer().position());
        assertEquals(100, bufRef.getBuffer().remaining());

        assertNull(context.getRpcCall());
        latch.await();
    }

    @Test
    public void testOnRpcReturnedOK() throws IOException {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.init(null, 0, bufRef);
        DownloadContext context = this.session.getDownloadManager().getDownloadContext(0);

        assertNull(context.getRpcCall());

        final FutureImpl<Message> future = new FutureImpl<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(100).setOffset(100).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), context.getDone()))
            .thenReturn(future);

        this.session.onRpcReturned(context, Status.OK(), RpcRequests.GetFileResponse.newBuilder().setReadSize(100)
            .setEof(false).setData(ByteString.copyFrom(new byte[100])).build());
        assertEquals(100, bufRef.capacity());
        assertEquals(100, bufRef.getBuffer().position());

        assertNotNull(context.getRpcCall());
        //send next request
        assertSame(future, context.getRpcCall());
    }

    @Test
    public void testOnRpcReturnedRetry() throws Exception {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(0);
        this.session.init(null, 0, bufRef);
        DownloadContext context = this.session.getDownloadManager().getDownloadContext(0);

        assertNull(context.getTimer());
        assertNull(context.getRpcCall());

        final FutureImpl<Message> future = new FutureImpl<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(100).setOffset(0).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), context.getDone()))
            .thenReturn(future);

        this.session.onRpcReturned(context, new Status(RaftError.EINTR, "test"), null);
        assertNotNull(context.getTimer());
        Thread.sleep(this.copyOpts.getRetryIntervalMs() + 100);
        assertNotNull(context.getRpcCall());
        assertSame(future, context.getRpcCall());
        assertNull(context.getTimer());
    }

    @Test
    public void testDownloadConcurrently() throws Exception {
        final String filePath = this.dataDir + File.separator + "data";
        final File file = new File(filePath);
        if(file.exists()) {
            file.delete();
        }
        final File stateFile = new File(filePath + ".state");
        if(stateFile.exists()) {
            stateFile.delete();
        }

        final long fileSize = 1024L * 1024 * 2;
        final byte[] fileData = new byte[(int) fileSize];
        new Random().nextBytes(fileData);
        ArgumentCaptor<RpcResponseClosure> argument = ArgumentCaptor.forClass(RpcResponseClosure.class);
        final FutureImpl<Message> future = new FutureImpl<>();
        Mockito.when(
            this.rpcService.getFile(eq(this.address), any(RpcRequests.GetFileRequest.class), eq(this.copyOpts.getTimeoutMs()), argument.capture()))
           .thenAnswer(new Answer<Future<Message>>() {

               @Override
               public Future<Message> answer(InvocationOnMock invocationOnMock) throws Throwable {
                   RpcRequests.GetFileRequest request = (RpcRequests.GetFileRequest) invocationOnMock.getArguments()[1];
//                   System.out.printf("GetFile, start: %d, end: %d\n", request.getOffset(), request.getOffset() + request.getCount());
                   CopySession.GetFileResponseClosure response =
                           (CopySession.GetFileResponseClosure) invocationOnMock.getArguments()[3];
                   boolean eof = request.getOffset() + request.getCount() == fileSize;
                   RpcRequests.GetFileResponse resp = RpcRequests.GetFileResponse.newBuilder().
                           setReadSize(request.getCount()).setEof(eof)
                           .setData(ByteString.copyFrom(fileData, (int) request.getOffset(), (int) request.getCount())).build();
                   RpcUtils.runInThread(() -> {
                       CopySessionTest.this.session.onRpcReturned(response.getContext(), Status.OK(), resp);
                   });
                   return future;
               }
           });

        raftOpts.setDownloadingSnapshotConcurrency(4);
        this.session.init(filePath, fileSize, null);
        this.session.start();
        this.session.join();
        assertTrue(this.session.status().isOk());
        assertEquals(fileSize, FileUtils.sizeOf(new File(filePath)));

        byte[] data = FileUtils.readFileToByteArray(new File(filePath));
        assertArrayEquals(fileData, data);

        assertFalse(stateFile.exists());
    }

    @Test
    public void testResumeDownloading() throws IOException {
        final String filePath = this.dataDir + File.separator + "data";
        final long fileSize = 1024L * 1024 * 2;
        raftOpts.setDownloadingSnapshotConcurrency(4);
        this.session.init(filePath, fileSize, null);
        List<DownloadContext> contexts = this.session.getDownloadManager().getDownloadContexts();

        final FutureImpl<Message> future = new FutureImpl<>();
        Mockito.when(this.rpcService.getFile(eq(this.address), any(), eq(this.copyOpts.getTimeoutMs()), any()))
            .thenReturn(future);
        for (DownloadContext context : contexts) {
            final long sliceSize = raftOpts.getMaxByteCountPerRpc();
            RpcRequests.GetFileResponse resp = RpcRequests.GetFileResponse.newBuilder().setReadSize(sliceSize)
                .setEof(false).setData(ByteString.copyFrom(new byte[(int) sliceSize])).build();
            this.session.onRpcReturned(context, Status.OK(), resp);
        }

        this.session.cancel();
        Utils.closeQuietly(this.session);

        this.session = new CopySession(rpcService, timerManager, null, raftOpts, rb, address);
        this.session.setCopyOptions(copyOpts);
        this.session.init(filePath, fileSize, null);
        contexts = this.session.getDownloadManager().getDownloadContexts();
        assertEquals(raftOpts.getMaxByteCountPerRpc(), contexts.get(0).currentOffset - contexts.get(0).begin);
        assertEquals(raftOpts.getMaxByteCountPerRpc(), contexts.get(1).currentOffset - contexts.get(1).begin);
        assertEquals(raftOpts.getMaxByteCountPerRpc(), contexts.get(2).currentOffset - contexts.get(2).begin);
        assertEquals(raftOpts.getMaxByteCountPerRpc(), contexts.get(3).currentOffset - contexts.get(3).begin);
    }

    private void sendNextRpc(int maxCount) {
        DownloadContext context = this.session.getDownloadManager().getDownloadContext(0);

        assertNull(context.getRpcCall());
        final FutureImpl<Message> future = new FutureImpl<>();
        final RpcRequests.GetFileRequest.Builder rb = RpcRequests.GetFileRequest.newBuilder().setReaderId(99)
            .setFilename("data").setCount(maxCount).setOffset(0).setReadPartly(true);
        Mockito
            .when(this.rpcService.getFile(this.address, rb.build(), this.copyOpts.getTimeoutMs(), context.getDone()))
            .thenReturn(future);
        this.session.sendNextRpc(context);
        assertNotNull(context.getRpcCall());
        assertSame(future, context.getRpcCall());
    }
}
