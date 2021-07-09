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

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.CrcUtil;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DownloadManager {
    private static final Logger   LOG         = LoggerFactory.getLogger(CopySession.class);

    private ByteBufferCollector   destBuf;
    private String                destPath;
    private FileChannel           destFileChannel;
    private final String          stateFileName;
    private final long            fileSize;
    private int                   concurrency;
    private List<DownloadContext> contextList = new ArrayList<>();
    private final Lock            lock        = new ReentrantLock();
    private final RaftOptions     raftOptions;
    private boolean               canceled;
    private boolean               finished;

    /**
     * if you need to download in parallel, | fileSize | must not be zero.
     * @param rb
     * @param raftOptions
     * @param destPath absolute file path
     * @param fileSize
     * @param concurrency
     */
    public DownloadManager(CopySession copySession, GetFileRequest.Builder rb, RaftOptions raftOptions,
                           String destPath, long fileSize, int concurrency) {
        this.raftOptions = raftOptions;
        if (destPath != null) {
            this.stateFileName = destPath + ".state";
        } else {
            this.stateFileName = null;
        }
        this.fileSize = fileSize;
        this.concurrency = concurrency;
        if (fileSize > 0) {
            long segmentLen = fileSize / concurrency;
            for (int i = 0; i < concurrency; i++) {
                DownloadContext context = new DownloadContext(copySession, rb);
                context.currentOffset = i * segmentLen;
                context.begin = i * segmentLen;
                context.lastOffset = context.currentOffset + segmentLen;
                contextList.add(context);
            }

            // Last connection downloads remaining bytes
            long remain = fileSize % segmentLen;
            contextList.get(contextList.size() - 1).lastOffset += remain;
        } else {
            this.concurrency = 1;
            DownloadContext context = new DownloadContext(copySession, rb);
            contextList.add(context);
        }
    }

    public DownloadContext getDownloadContext(int index) {
        return contextList.get(index);
    }

    @OnlyForTest
    public List<DownloadContext> getDownloadContexts() {
        return contextList;
    }

    public boolean stealOtherSegment(DownloadContext idleContext) {
        long maxRemain = this.raftOptions.getMaxByteCountPerRpc();
        DownloadContext selected = null;
        lock.lock();
        try {
            if (canceled || finished) {
                return false;
            }
            for (int i = 0; i < concurrency; i++) {
                DownloadContext current = contextList.get(i);
                if (current == idleContext)
                    continue;

                long remain = current.lastOffset - current.currentOffset;
                if (remain > maxRemain) {
                    selected = current;
                    maxRemain = remain;
                }
            }

            if (selected == null) {
                return false;
            }
            long connLastOffset = 0;
            long connCurrentOffset = 0;
            selected.lock.lock();
            maxRemain = selected.lastOffset - selected.currentOffset;
            if (maxRemain < this.raftOptions.getMaxByteCountPerRpc()) {
                maxRemain = 0;
            } else {
                connLastOffset = selected.lastOffset;
                connCurrentOffset = selected.currentOffset + maxRemain / 2;
                selected.lastOffset = connCurrentOffset;
            }
            selected.lock.unlock();

            if (maxRemain > 0) {
                idleContext.lock.lock();
                idleContext.lastOffset = connLastOffset;
                idleContext.currentOffset = connCurrentOffset;
                idleContext.begin = connCurrentOffset;
                idleContext.lock.unlock();
                LOG.debug("steal download snapshot task, maxRemain: {} new start: {}, new end: {}", maxRemain,
                    connCurrentOffset, connLastOffset);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    public void saveState() {
        this.lock.lock();
        try {
            if (destFileChannel != null) {
                destFileChannel.force(true);
            }
            this.saveToFile();
        } catch (IOException e) {
            LOG.error("sync snapshot file error, {}", e.getMessage());
        } finally {
            this.lock.unlock();
        }
    }

    private void saveToFile() {
        if (this.stateFileName == null || this.finished) {
            return;
        }

        int dataSize = 4 + 16 * concurrency;

        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        buffer.putInt(concurrency);

        for (int i = 0; i < concurrency; i++) {
            DownloadContext conn = contextList.get(i);
            buffer.putLong(conn.currentOffset);
            buffer.putLong(conn.lastOffset);
        }

        buffer.flip();

        long checksum = CrcUtil.crc64(buffer);
        ByteBuffer checksumBuffer = ByteBuffer.allocate(8);
        checksumBuffer.putLong(checksum);
        checksumBuffer.flip();

        try (FileChannel fileChannel = FileChannel.open(Paths.get(stateFileName), StandardOpenOption.WRITE,
            StandardOpenOption.CREATE)) {

            fileChannel.write(checksumBuffer);
            fileChannel.write(buffer);
            fileChannel.force(false);
        } catch (IOException e) {
            LOG.error("save download state file error, error: {}", e.getMessage());
        }

    }

    private void loadFromFile() {
        if (this.stateFileName == null) {
            return;
        }
        ByteBuffer buffer;
        try (FileChannel fileChannel = FileChannel.open(Paths.get(this.stateFileName), StandardOpenOption.READ)) {

            buffer = ByteBuffer.allocate((int) fileChannel.size());
            fileChannel.read(buffer);
            buffer.flip();
        } catch (IOException ignored) {
            return;
        }

        if (buffer.remaining() < 8) {
            return;
        }
        long checksum = buffer.getLong();
        if (CrcUtil.crc64(buffer) != checksum) {
            return;
        }

        int concurrency = buffer.getInt();

        // not support concurrency change
        Requires.requireTrue(this.concurrency == concurrency);
        for (int i = 0; i < concurrency; i++) {
            DownloadContext context = contextList.get(i);
            context.currentOffset = buffer.getLong();
            context.lastOffset = buffer.getLong();
            LOG.debug("download context[{}] start: {} end:{}", i, context.currentOffset, context.lastOffset);
        }
    }

    /**
     * if dest is a buffer, the concurrency is one.
     * @param bufRef
     */
    public void setDestBuf(ByteBufferCollector bufRef) {
        Requires.requireTrue(destPath == null, "only one of destPath or destBuf can be set.");
        if (bufRef != null) {
            assert this.concurrency == 1;
            this.destBuf = bufRef;
            contextList.get(0).destBuf = bufRef;
        }
    }

    /**
     * set the dest file path, and try to load old state
     * @param destPath file path to download
     * @throws IOException
     */
    public void setDestPath(String destPath) throws IOException {
        Requires.requireTrue(destBuf == null, "only one of destPath or destBuf can be set.");
        if (destPath == null) {
            return;
        }
        this.destPath = destPath;
        Requires.requireTrue(this.fileSize > 0, "fileSize must not be zero!");

        File file = new File(destPath);
        try {
            for (int i = 0; i < concurrency; i++) {
                DownloadContext context = contextList.get(i);
                FileChannel fileChannel;

                if (file.exists()) {
                    fileChannel = FileChannel.open(Paths.get(destPath), StandardOpenOption.WRITE);
                } else {
                    fileChannel = FileChannel.open(Paths.get(destPath), StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE);
                    fileChannel.truncate(fileSize);
                }
                context.fileChannel = fileChannel;
                if (this.destFileChannel == null) {
                    this.destFileChannel = fileChannel;
                }
            }
        } catch (IOException e) {
            for (int i = 0; i < concurrency; i++) {
                DownloadContext context = contextList.get(i);
                if (context.fileChannel != null) {
                    Utils.closeQuietly(context.fileChannel);
                }
            }
            throw e;
        }

        loadFromFile();
    }

    public void stop() {
        this.lock.lock();
        try {
            if (!finished) {
                this.saveToFile();
                canceled = true;
                finished = true;
                destFileChannel = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void downloadFinished() {
        this.lock.lock();
        try {
            boolean completed = true;
            for (DownloadContext downloadContext : contextList) {
                if (downloadContext.currentOffset < downloadContext.lastOffset) {
                    completed = false;
                }
            }
            finished = true;
            destFileChannel = null;
            if (this.stateFileName == null || !completed) {
                return;
            }
            File file = new File(this.stateFileName);
            if (file.exists()) {
                // TODO When is it appropriate to delete this file?
                file.delete();
            }
        } finally {
            this.lock.unlock();
        }
    }
}
