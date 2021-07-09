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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.Scheduler;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CopyOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.RpcUtils;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Copy session.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 12:01:23 PM
 */
@ThreadSafe
public class CopySession implements Session {

    private static final Logger          LOG         = LoggerFactory.getLogger(CopySession.class);

    private final Lock                   lock        = new ReentrantLock();
    private final Status                 st          = Status.OK();
    private CountDownLatch               finishLatch;
    private final RaftClientService      rpcService;
    private final GetFileRequest.Builder requestBuilder;
    private final Endpoint               endpoint;
    private final Scheduler              timerManager;
    private final SnapshotThrottle       snapshotThrottle;
    private final RaftOptions            raftOptions;
    private CopyOptions                  copyOptions = new CopyOptions();
    private String                       destPath;
    private DownloadManager              downloadManager;
    private int                          concurrency;
    private ScheduledFuture<?>           saveStateFuture;
    private AtomicInteger                remainTask;

    /**
     * Get file response closure to answer client.
     *
     * @author boyan (boyan@alibaba-inc.com)
     */
    static class GetFileResponseClosure extends RpcResponseClosureAdapter<GetFileResponse> {
        final DownloadContext context;
        final CopySession     copySession;

        public GetFileResponseClosure(CopySession copySession, DownloadContext context) {
            this.copySession = copySession;
            this.context = context;
        }

        @OnlyForTest
        public DownloadContext getContext() {
            return context;
        }

        @Override
        public void run(final Status status) {
            copySession.onRpcReturned(context, status, getResponse());
        }
    }

    public void setDestPath(final String destPath) {
        this.destPath = destPath;
    }

    @OnlyForTest
    DownloadManager getDownloadManager() {
        return this.downloadManager;
    }

    @Override
    public void close() throws IOException {
        for (int i = 0; i < concurrency; i++) {
            DownloadContext context = downloadManager.getDownloadContext(i);
            context.lock.lock();
            try {
                if (!context.finished) {
                    Utils.closeQuietly(context.fileChannel);
                }
                if (context.destBuf != null) {
                    context.destBuf.recycle();
                    context.destBuf = null;
                }
            } finally {
                context.lock.unlock();
            }
        }
        this.lock.lock();
        try {
            if (this.saveStateFuture != null) {
                this.saveStateFuture.cancel(true);
                this.saveStateFuture = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    public CopySession(final RaftClientService rpcService, final Scheduler timerManager,
                       final SnapshotThrottle snapshotThrottle, final RaftOptions raftOptions,
                       final GetFileRequest.Builder rb, final Endpoint ep) {
        super();
        this.snapshotThrottle = snapshotThrottle;
        this.raftOptions = raftOptions;
        this.timerManager = timerManager;
        this.rpcService = rpcService;
        this.requestBuilder = rb;
        this.endpoint = ep;
    }

    public void setCopyOptions(final CopyOptions copyOptions) {
        this.copyOptions = copyOptions;
    }

    public void init(String destPath, long fileSize, final ByteBufferCollector bufRef) throws IOException {
        this.concurrency = bufRef != null ? 1 : raftOptions.getDownloadingSnapshotConcurrency();
        long sliceCount = fileSize == 0 ? 1 : (fileSize - 1) / raftOptions.getMaxByteCountPerRpc() + 1;
        this.concurrency = (int) Math.min(this.concurrency, sliceCount);

        this.finishLatch = new CountDownLatch(this.concurrency);
        this.remainTask = new AtomicInteger(this.concurrency);

        this.downloadManager = new DownloadManager(this, this.requestBuilder, this.raftOptions, destPath, fileSize,
            this.concurrency);
        this.downloadManager.setDestBuf(bufRef);
        this.downloadManager.setDestPath(destPath);
    }

    @Override
    public void cancel() {
        this.lock.lock();
        try {
            for (int i = 0; i < concurrency; i++) {
                DownloadContext context = downloadManager.getDownloadContext(i);
                context.lock.lock();
                try {
                    if (context.finished) {
                        continue;
                    }
                    if (context.timer != null) {
                        context.timer.cancel(true);
                    }
                    if (context.rpcCall != null) {
                        context.rpcCall.cancel(true);
                    }
                    if (context.st.isOk()) {
                        context.st.setError(RaftError.ECANCELED, RaftError.ECANCELED.name());
                    }
                    if (context.destBuf != null) {
                        context.destBuf.recycle();
                        context.destBuf = null;
                    }
                } finally {
                    context.lock.unlock();
                }
                onFinished(context);
            }
            if (saveStateFuture != null) {
                saveStateFuture.cancel(true);
                saveStateFuture = null;
            }
            downloadManager.stop();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void join() throws InterruptedException {
        this.finishLatch.await();
    }

    @Override
    public Status status() {
        synchronized (this.st) {
            return this.st;
        }
    }

    private void onFinished(DownloadContext context) {
        context.lock.lock();

        boolean canSteal = !context.finished && context.st.isOk();
        context.lock.unlock();
        if (canSteal) {
            if (downloadManager.stealOtherSegment(context)) {
                sendNextRpc(context);
                return;
            }
        }

        boolean downloadFinished = false;
        context.lock.lock();
        try {
            if (!context.finished) {
                if (!context.st.isOk()) {
                    LOG.error("Fail to copy data, readerId={} fileName={} offset={} status={}",
                        context.requestBuilder.getReaderId(), context.requestBuilder.getFilename(),
                        context.requestBuilder.getOffset(), context.st);
                    synchronized (this.st) {
                        if (this.st.isOk()) {
                            this.st.setError(context.st.getCode(), context.st.getErrorMsg());
                        }
                    }
                }
                if (remainTask.decrementAndGet() == 0) {
                    downloadFinished = true;
                }

                if (context.fileChannel != null) {
                    Utils.closeQuietly(context.fileChannel);
                    context.fileChannel = null;
                }
                if (context.destBuf != null) {
                    final ByteBuffer buf = context.destBuf.getBuffer();
                    if (buf != null) {
                        buf.flip();
                    }
                    context.destBuf = null;
                }
                context.finished = true;
                this.finishLatch.countDown();
            }
        } finally {
            context.lock.unlock();
            if (downloadFinished) {
                downloadManager.downloadFinished();
                LOG.info("download finished");
                this.lock.lock();
                try {
                    if (saveStateFuture != null) {
                        saveStateFuture.cancel(true);
                        saveStateFuture = null;
                    }
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    private void onTimer(final DownloadContext context) {
        RpcUtils.runInThread(() -> sendNextRpc(context));
    }

    private void onSaveState() {
        downloadManager.saveState();
    }

    void onRpcReturned(final DownloadContext context, final Status status, final GetFileResponse response) {
        try {
            int mSec = RandomUtils.nextInt(10);
            Thread.sleep(10 + mSec);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        boolean finished = false;
        context.lock.lock();
        try {
            if (context.finished) {
                return;
            }
            if (!status.isOk()) {
                // Reset count to make next rpc retry the previous one
                context.requestBuilder.setCount(0);
                if (status.getCode() == RaftError.ECANCELED.getNumber()) {
                    if (context.st.isOk()) {
                        context.st.setError(status.getCode(), status.getErrorMsg());
                        finished = true;
                        return;
                    }
                }

                // Throttled reading failure does not increase _retry_times
                if (status.getCode() != RaftError.EAGAIN.getNumber()
                        && ++context.retryTimes >= this.copyOptions.getMaxRetry()) {
                    if (context.st.isOk()) {
                        context.st.setError(status.getCode(), status.getErrorMsg());
                        finished = true;
                        return;
                    }
                }
                context.timer = this.timerManager.schedule(() -> onTimer(context), this.copyOptions.getRetryIntervalMs(),
                    TimeUnit.MILLISECONDS);
                return;
            }
            context.retryTimes = 0;
            Requires.requireNonNull(response, "response");
            // Reset count to |real_read_size| to make next rpc get the right offset
            if (!response.getEof()) {
                context.requestBuilder.setCount(response.getReadSize());
            }
            if (context.fileChannel != null) {
                try {
                    context.fileChannel.write(response.getData().asReadOnlyByteBuffer(), context.currentOffset);
                } catch (final IOException e) {
                    LOG.error("Fail to write into file {}", this.destPath);
                    context.st.setError(RaftError.EIO, RaftError.EIO.name());
                    finished = true;
                    return;
                }
                context.currentOffset += response.getData().size();
                // Due to work stealing, |lastOffset| may become less
                if (context.currentOffset >= context.lastOffset) {
                    finished = true;
                    return;
                }
            } else {
                context.destBuf.put(response.getData().asReadOnlyByteBuffer());
                context.currentOffset += response.getData().size();
                if(response.getEof()) {
                    finished = true;
                    return;
                }
            }
        } finally {
            context.lock.unlock();
            if(finished) {
                onFinished(context);
            }
        }
        sendNextRpc(context);
    }

    /**
     * Send next RPC request to get a piece of file data.
     */
    void sendNextRpc(final DownloadContext context) {
        context.lock.lock();
        try {
            context.timer = null;
            final long offset = context.currentOffset;
            final long remainBytes = context.destBuf != null ? Integer.MAX_VALUE : context.lastOffset - context.currentOffset;
            final long maxCount = Math.min(this.raftOptions.getMaxByteCountPerRpc(), remainBytes);

            context.requestBuilder.setOffset(offset).setCount(maxCount).setReadPartly(true);

            if (context.finished) {
                return;
            }
            // throttle
            long newMaxCount = maxCount;
            if (this.snapshotThrottle != null) {
                newMaxCount = this.snapshotThrottle.throttledByThroughput(maxCount);
                if (newMaxCount == 0) {
                    // Reset count to make next rpc retry the previous one
                    context.requestBuilder.setCount(0);
                    context.timer = this.timerManager.schedule(() -> onTimer(context), this.copyOptions.getRetryIntervalMs(),
                        TimeUnit.MILLISECONDS);
                    return;
                }
            }
            context.requestBuilder.setCount(newMaxCount);
            final RpcRequests.GetFileRequest request = context.requestBuilder.build();
//            LOG.debug("Send get file request {} to peer {}", request, this.endpoint);
            context.rpcCall = this.rpcService.getFile(this.endpoint, request, this.copyOptions.getTimeoutMs(), context.done);
        } finally {
            context.lock.unlock();
        }
    }

    public void start() {
        for (int i = 0; i < this.concurrency; i++) {
            DownloadContext context = this.downloadManager.getDownloadContext(i);
            sendNextRpc(context);
        }
        this.saveStateFuture = this.timerManager.scheduleWithFixedDelay(this::onSaveState,
                1, 1, TimeUnit.SECONDS);
    }
}
