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
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.test.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class DownloadManagerTest {
    private DownloadManager                    downloadManager;

    private RaftOptions                        raftOpts;
    private String                             dataDir;
    private String                             destFilePath;
    private final int                          fileSize            = 2 * 1024 * 1024;
    private final int                          sliceSize           = 128 * 1024;
    private final int                          downloadConcurrency = 4;
    private RpcRequests.GetFileRequest.Builder rb;

    private DownloadManager newDownloadManager() throws IOException {
        DownloadManager manager = new DownloadManager(null, rb, raftOpts, destFilePath, fileSize, downloadConcurrency);
        manager.setDestPath(destFilePath);
        return manager;
    }

    @Before
    public void setup() throws IOException {
        rb = RpcRequests.GetFileRequest.newBuilder();
        this.raftOpts = new RaftOptions();
        raftOpts.setDownloadingSnapshotConcurrency(downloadConcurrency);
        this.dataDir = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataDir));
        this.destFilePath = dataDir + File.separator + "data";
        this.downloadManager = newDownloadManager();
    }

    @Test
    public void testSteal() {
        DownloadContext context0 = downloadManager.getDownloadContext(0);
        DownloadContext context1 = downloadManager.getDownloadContext(1);
        DownloadContext context2 = downloadManager.getDownloadContext(2);
        DownloadContext context3 = downloadManager.getDownloadContext(3);
        context0.currentOffset = context0.lastOffset;
        context1.currentOffset += sliceSize;
        context2.currentOffset += sliceSize;

        long oldCurrentOffset3 = context3.currentOffset;
        long oldLastOffset3 = context3.lastOffset;

        downloadManager.stealOtherSegment(context0);
        assertEquals(context0.begin, context0.currentOffset);
        assertEquals(context3.lastOffset, context0.begin);
        assertEquals(oldLastOffset3, context0.lastOffset);
        assertEquals(oldCurrentOffset3, context3.currentOffset);
    }

    @Test
    public void testResume() throws IOException {
        List<Long> oldOffset = new ArrayList<>();
        for (int i = 0; i < downloadConcurrency; i++) {
            oldOffset.add(downloadManager.getDownloadContext(i).currentOffset);
            downloadManager.getDownloadContext(i).currentOffset += sliceSize;
        }
        downloadManager.saveState();
        downloadManager.stop();
        downloadManager = newDownloadManager();

        for (int i = 0; i < downloadConcurrency; i++) {
            long expect = oldOffset.get(i) + sliceSize;
            assertEquals(expect, downloadManager.getDownloadContext(i).currentOffset);
        }
    }

    @Test
    public void testCompleted() {
        String stateFilePath = this.destFilePath + ".state";
        assertFalse(new File(stateFilePath).exists());
        downloadManager.saveState();
        assertTrue(new File(stateFilePath).exists());

        for (int i = 0; i < downloadConcurrency; i++) {
            DownloadContext context = downloadManager.getDownloadContext(i);
            context.currentOffset = context.lastOffset;
        }

        downloadManager.downloadFinished();
        assertFalse(new File(stateFilePath).exists());
    }

}
