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
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alipay.sofa.jraft.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.FileSource;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.remote.RemoteFileCopier;
import com.alipay.sofa.jraft.storage.snapshot.remote.Session;

/**
 * Copy another machine snapshot to local.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-07 11:32:30 AM
 */
public class LocalSnapshotCopier extends SnapshotCopier {

    private static final Logger          LOG             = LoggerFactory.getLogger(LocalSnapshotCopier.class);

    private final Lock                   lock            = new ReentrantLock();
    /** The copy job future object*/
    private volatile Future<?>           future;
    private boolean                      cancelled;
    /** snapshot writer */
    private LocalSnapshotWriter          writer;
    /** snapshot reader */
    private volatile LocalSnapshotReader reader;
    /** snapshot storage*/
    private LocalSnapshotStorage         storage;
    private boolean                      filterBeforeCopyRemote;
    private LocalSnapshot                remoteSnapshot;
    /** remote file copier*/
    private RemoteFileCopier             copier;
    /** current copying session*/
    private List<Session>                sessions        = new CopyOnWriteArrayList<>();
    private SnapshotThrottle             snapshotThrottle;
    private Map<String, CountDownLatch>  sliceMergeLatch = new HashMap<>();
    private Executor                     executor;
    private static final String          SLICE_SEPARATOR = "_";
    private static final String          EXECUTOR_NAME   = "SNAPSHOT_COPIER_EXECUTOR_";

    public void setSnapshotThrottle(final SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    private void startCopy() {
        try {
            internalCopy();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); //reset/ignore
        } catch (final IOException e) {
            LOG.error("Fail to start copy job", e);
        }
    }

    private void internalCopy() throws IOException, InterruptedException {
        CountDownLatch closeLatch = null;
        // noinspection ConstantConditions
        do {
            loadMetaTable();
            if (!isOk()) {
                break;
            }
            filter();
            if (!isOk()) {
                break;
            }
            final Set<String> files = this.remoteSnapshot.listFiles();
            closeLatch = new CountDownLatch(files.size());
            for (final String file : files) {
                LocalFileMeta fileMeta = this.remoteSnapshot.getMetaTable().getFileMeta(file);
                int sliceTotal = fileMeta.getSliceTotal();
                sliceMergeLatch.put(file, new CountDownLatch(sliceTotal));

                executor.execute(() -> copyFile(file));
                sliceMergeLatch.get(file).await();
                mergeSlice(file, sliceTotal);
                if (isOk() && !this.writer.addFile(file, fileMeta)) {
                    setError(RaftError.EIO, "Fail to add file to writer");
                    return;
                }
                closeLatch.countDown();
            }
        } while (false);

        if (closeLatch != null) {
            closeLatch.await();
        }

        if (!isOk() && this.writer != null && this.writer.isOk()) {
            this.writer.setError(getCode(), getErrorMsg());
        }
        if (this.writer != null) {
            Utils.closeQuietly(this.writer);
            this.writer = null;
        }
        if (isOk()) {
            this.reader = (LocalSnapshotReader) this.storage.open();
        }
    }

    private void mergeSlice(String filename, int sliceTotal) {
        String filePath = this.writer.getPath() + File.separator + filename;

        //Solve the problem of abnormal interruption in the process of file merging
        //Delete incomplete merged files
        FileUtils.deleteQuietly(new File(filePath));

        //Merge file
        boolean success = true;
        for (int i = 0; i < sliceTotal; i++) {
            try (InputStream inputStream = new FileInputStream(filePath + SLICE_SEPARATOR + i);
                    OutputStream outputStream = new FileOutputStream(filePath, true)) {
                IOUtils.copy(inputStream, outputStream);
            } catch (IOException e) {
                String message = String.format("Merge slice file %s IOException", filename + SLICE_SEPARATOR + i);
                this.setError(-1, message);
                LOG.error(message, e);
                success = false;
                break;
            }
        }

        //Delete file slice after file merge
        if (success) {
            for (int i = 0; i < sliceTotal; i++) {
                FileUtils.deleteQuietly(new File(filePath + SLICE_SEPARATOR + i));
                LOG.debug("Delete slice" + filePath + SLICE_SEPARATOR + i);
            }
        }
    }

    void copyFile(final String fileName) {
        if (this.writer.getFileMeta(fileName) != null) {
            LOG.info("Skipped downloading {}", fileName);
            return;
        }
        if (!checkFile(fileName)) {
            return;
        }

        String filePath = this.writer.getPath() + File.separator + fileName;
        Path subPath = Paths.get(filePath);
        if (!subPath.equals(subPath.getParent()) && !subPath.getParent().getFileName().toString().equals(".")) {
            final File parentDir = subPath.getParent().toFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                LOG.error("Fail to create directory for {}", filePath);
                setError(RaftError.EIO, "Fail to create directory");
                return;
            }
        }

        this.lock.lock();
        try {
            if (this.cancelled) {
                if (isOk()) {
                    setError(RaftError.ECANCELED, "ECANCELED");
                }
                return;
            }
        } finally {
            this.lock.unlock();
        }
        LocalFileMeta fileMeta = (LocalFileMeta) remoteSnapshot.getFileMeta(fileName);
        int sliceTotal = fileMeta.getSliceTotal();
        for (int i = 0; i < sliceTotal; i++) {
            final int finalI = i;
            final String finalPath = filePath + SLICE_SEPARATOR + finalI;
            executor.execute(() -> {
                Session session = null;
                try {
                    long offset;
                    try {
                        offset = FileUtils.sizeOf(new File(finalPath));
                    } catch (IllegalArgumentException e) {
                        offset = 0;
                    }
                    try {
                        session = this.copier.startCopyToFile(fileName,
                                finalPath, finalI, offset, null);
                    } catch (IOException e) {
                        LOG.error("Copy file {} IOException", filePath + SLICE_SEPARATOR + finalI);
                    }
                    if (session == null) {
                        LOG.error("Fail to copy {}", fileName);
                        setError(-1, "Fail to copy %s", fileName);
                        return;
                    }
                    this.sessions.add(session);

                    try {
                        session.join(); // join out of lock
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    this.sessions.remove(session);
                    sliceMergeLatch.get(fileName).countDown();

                    if (!session.status().isOk() && isOk()) {
                        setError(session.status().getCode(), session.status().getErrorMsg());
                    }
                } finally {
                    if (session != null) {
                        Utils.closeQuietly(session);
                    }
                }
            });
        }
    }

    private boolean checkFile(final String fileName) {
        try {
            final String parentCanonicalPath = Paths.get(this.writer.getPath()).toFile().getCanonicalPath();
            final Path filePath = Paths.get(parentCanonicalPath, fileName);
            final File file = filePath.toFile();
            final String fileAbsolutePath = file.getAbsolutePath();
            final String fileCanonicalPath = file.getCanonicalPath();
            if (!fileAbsolutePath.equals(fileCanonicalPath)) {
                LOG.error("File[{}] are not allowed to be created outside of directory[{}].", fileAbsolutePath,
                    fileCanonicalPath);
                setError(RaftError.EIO, "File[%s] are not allowed to be created outside of directory.",
                    fileAbsolutePath, fileCanonicalPath);
                return false;
            }
        } catch (final IOException e) {
            LOG.error("Failed to check file: {}, writer path: {}.", fileName, this.writer.getPath(), e);
            setError(RaftError.EIO, "Failed to check file: {}, writer path: {}.", fileName, this.writer.getPath());
            return false;
        }
        return true;
    }

    private void loadMetaTable() throws InterruptedException {
        final ByteBufferCollector metaBuf = ByteBufferCollector.allocate(0);
        Session session = null;
        try {
            this.lock.lock();
            try {
                if (this.cancelled) {
                    if (isOk()) {
                        setError(RaftError.ECANCELED, "ECANCELED");
                    }
                    return;
                }
                session = this.copier.startCopy2IoBuffer(Snapshot.JRAFT_SNAPSHOT_META_FILE, metaBuf, null);
                this.sessions.add(session);
            } finally {
                this.lock.unlock();
            }
            session.join(); //join out of lock.
            this.lock.lock();
            try {
                this.sessions.remove(session);
            } finally {
                this.lock.unlock();
            }
            if (!session.status().isOk() && isOk()) {
                LOG.warn("Fail to copy meta file: {}", session.status());
                setError(session.status().getCode(), session.status().getErrorMsg());
                return;
            }
            if (!this.remoteSnapshot.getMetaTable().loadFromIoBufferAsRemote(metaBuf.getBuffer())) {
                LOG.warn("Bad meta_table format");
                setError(-1, "Bad meta_table format from remote");
                return;
            }
            Requires.requireTrue(this.remoteSnapshot.getMetaTable().hasMeta(), "Invalid remote snapshot meta:%s",
                this.remoteSnapshot.getMetaTable().getMeta());
        } finally {
            if (session != null) {
                Utils.closeQuietly(session);
            }
        }
    }

    boolean filterBeforeCopy(final LocalSnapshotWriter writer, final SnapshotReader lastSnapshot) throws IOException {
        final Set<String> existingFiles = writer.listFiles();
        final ArrayDeque<String> toRemove = new ArrayDeque<>();
        for (final String file : existingFiles) {
            if (this.remoteSnapshot.getFileMeta(file) == null) {
                toRemove.add(file);
                writer.removeFile(file);
            }
        }

        final Set<String> remoteFiles = this.remoteSnapshot.listFiles();

        for (final String fileName : remoteFiles) {
            final LocalFileMeta remoteMeta = (LocalFileMeta) this.remoteSnapshot.getFileMeta(fileName);
            Requires.requireNonNull(remoteMeta, "remoteMeta");
            if (!remoteMeta.hasChecksum()) {
                // Re-download file if this file doesn't have checksum
                writer.removeFile(fileName);
                toRemove.add(fileName);
                continue;
            }

            LocalFileMeta localMeta = (LocalFileMeta) writer.getFileMeta(fileName);
            if (localMeta != null) {
                if (localMeta.hasChecksum() && localMeta.getChecksum().equals(remoteMeta.getChecksum())) {
                    LOG.info("Keep file={} checksum={} in {}", fileName, remoteMeta.getChecksum(), writer.getPath());
                    continue;
                }
                // Remove files from writer so that the file is to be copied from
                // remote_snapshot or last_snapshot
                writer.removeFile(fileName);
                toRemove.add(fileName);
            }
            // Try find files in last_snapshot
            if (lastSnapshot == null) {
                continue;
            }
            if ((localMeta = (LocalFileMeta) lastSnapshot.getFileMeta(fileName)) == null) {
                continue;
            }
            if (!localMeta.hasChecksum() || !localMeta.getChecksum().equals(remoteMeta.getChecksum())) {
                continue;
            }

            LOG.info("Found the same file ={} checksum={} in lastSnapshot={}", fileName, remoteMeta.getChecksum(),
                lastSnapshot.getPath());
            if (localMeta.getSource() == FileSource.FILE_SOURCE_LOCAL) {
                final String sourcePath = lastSnapshot.getPath() + File.separator + fileName;
                final String destPath = writer.getPath() + File.separator + fileName;
                FileUtils.deleteQuietly(new File(destPath));
                try {
                    Files.createLink(Paths.get(destPath), Paths.get(sourcePath));
                } catch (final IOException e) {
                    LOG.error("Fail to link {} to {}", sourcePath, destPath, e);
                    continue;
                }
                // Don't delete linked file
                if (!toRemove.isEmpty() && toRemove.peekLast().equals(fileName)) {
                    toRemove.pollLast();
                }
            }
            // Copy file from last_snapshot
            writer.addFile(fileName, localMeta);
        }
        if (!writer.sync()) {
            LOG.error("Fail to sync writer on path={}", writer.getPath());
            return false;
        }
        for (final String fileName : toRemove) {
            final String removePath = writer.getPath() + File.separator + fileName;
            FileUtils.deleteQuietly(new File(removePath));
            LOG.info("Deleted file: {}", removePath);
        }
        return true;
    }

    private void filter() throws IOException {
        this.writer = (LocalSnapshotWriter) this.storage.create(!this.filterBeforeCopyRemote);
        if (this.writer == null) {
            setError(RaftError.EIO, "Fail to create snapshot writer");
            return;
        }
        if (this.filterBeforeCopyRemote) {
            final SnapshotReader reader = this.storage.open();
            if (!filterBeforeCopy(this.writer, reader)) {
                LOG.warn("Fail to filter writer before copying, destroy and create a new writer.");
                this.writer.setError(-1, "Fail to filter");
                Utils.closeQuietly(this.writer);
                this.writer = (LocalSnapshotWriter) this.storage.create(true);
            }
            if (reader != null) {
                Utils.closeQuietly(reader);
            }
            if (this.writer == null) {
                setError(RaftError.EIO, "Fail to create snapshot writer");
                return;
            }
        }
        this.writer.saveMeta(this.remoteSnapshot.getMetaTable().getMeta());
        if (!this.writer.sync()) {
            LOG.error("Fail to sync snapshot writer path={}", this.writer.getPath());
            setError(RaftError.EIO, "Fail to sync snapshot writer");
        }
    }

    public boolean init(final String uri, final SnapshotCopierOptions opts) {
        this.copier = new RemoteFileCopier();
        this.cancelled = false;
        this.filterBeforeCopyRemote = opts.getNodeOptions().isFilterBeforeCopyRemote();
        this.remoteSnapshot = new LocalSnapshot(opts.getRaftOptions());
        this.executor = ThreadPoolUtil.newBuilder().poolName(EXECUTOR_NAME).enableMetric(true)
            .coreThreads(opts.getNodeOptions().getSnapshotCopierThreadPoolSize())
            .maximumThreads(opts.getNodeOptions().getSnapshotCopierThreadPoolSize()).keepAliveSeconds(60L)
            .workQueue(new LinkedBlockingQueue<>()).threadFactory(new NamedThreadFactory(EXECUTOR_NAME, true)).build();
        return this.copier.init(uri, this.snapshotThrottle, opts);
    }

    public SnapshotStorage getStorage() {
        return this.storage;
    }

    public void setStorage(final SnapshotStorage storage) {
        this.storage = (LocalSnapshotStorage) storage;
    }

    public boolean isFilterBeforeCopyRemote() {
        return this.filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(final boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

    @Override
    public void close() {
        cancel();
        try {
            join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void start() {
        this.future = Utils.runInThread(this::startCopy);
    }

    @Override
    public void cancel() {
        this.lock.lock();
        try {
            if (this.cancelled) {
                return;
            }
            if (isOk()) {
                setError(RaftError.ECANCELED, "Cancel the copier manually.");
            }
            this.cancelled = true;

            for (Session session : sessions) {
                session.cancel();
                sessions.remove(session);
            }
            if (this.future != null) {
                this.future.cancel(true);
            }
        } finally {
            this.lock.unlock();
        }

    }

    @Override
    public void join() throws InterruptedException {
        if (this.future != null) {
            try {
                this.future.get();
            } catch (final InterruptedException e) {
                throw e;
            } catch (final CancellationException ignored) {
                // ignored
            } catch (final Exception e) {
                LOG.error("Fail to join on copier", e);
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public SnapshotReader getReader() {
        return this.reader;
    }
}
