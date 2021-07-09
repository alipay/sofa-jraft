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
import com.alipay.sofa.jraft.rpc.RpcRequests.GetFileRequest;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.google.protobuf.Message;

import java.nio.channels.FileChannel;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DownloadContext {
    final Lock                               lock       = new ReentrantLock();
    final Status                             st         = Status.OK();
    int                                      retryTimes = 0;

    final CopySession.GetFileResponseClosure done;
    final GetFileRequest.Builder             requestBuilder;
    FileChannel                              fileChannel;
    ByteBufferCollector                      destBuf;

    boolean                                  finished;
    Future<Message>                          rpcCall;
    ScheduledFuture<?>                       timer;
    long                                     begin;
    long                                     currentOffset;
    long                                     lastOffset;

    public DownloadContext(CopySession copySession, GetFileRequest.Builder requestBuilder) {
        this.done = new CopySession.GetFileResponseClosure(copySession, this);
        this.requestBuilder = requestBuilder.clone();
    }

    @OnlyForTest
    CopySession.GetFileResponseClosure getDone() {
        return this.done;
    }

    @OnlyForTest
    Future<Message> getRpcCall() {
        return this.rpcCall;
    }

    @OnlyForTest
    ScheduledFuture<?> getTimer() {
        return this.timer;
    }
}
