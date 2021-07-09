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
package com.alipay.sofa.jraft.core;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.core.Replicator.RequestType;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReplicatorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.impl.FutureImpl;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Matchers.any;

@RunWith(value = MockitoJUnitRunner.class)
public class ReplicatorTest {

    private ThreadId          id;
    private final RaftOptions raftOptions = new RaftOptions();
    private TimerManager      timerManager;
    @Mock
    private RaftClientService rpcService;
    @Mock
    private NodeImpl          node;
    @Mock
    private BallotBox         ballotBox;
    @Mock
    private LogManager        logManager;
    @Mock
    private SnapshotStorage   snapshotStorage;
    private ReplicatorOptions opts;
    private final PeerId      peerId      = new PeerId("localhost", 8081);

    @Before
    public void setup() {
        this.timerManager = new TimerManager(5);
        this.opts = new ReplicatorOptions();
        this.opts.setRaftRpcService(this.rpcService);
        this.opts.setPeerId(this.peerId);
        this.opts.setBallotBox(this.ballotBox);
        this.opts.setGroupId("test");
        this.opts.setTerm(1);
        this.opts.setServerId(new PeerId("localhost", 8082));
        this.opts.setNode(this.node);
        this.opts.setSnapshotStorage(this.snapshotStorage);
        this.opts.setTimerManager(this.timerManager);
        this.opts.setLogManager(this.logManager);
        this.opts.setDynamicHeartBeatTimeoutMs(100);
        this.opts.setElectionTimeoutMs(1000);

        Mockito.when(this.logManager.getLastLogIndex()).thenReturn(10L);
        Mockito.when(this.logManager.getTerm(10)).thenReturn(1L);
        Mockito.when(this.rpcService.connect(this.peerId.getEndpoint())).thenReturn(true);
        Mockito.when(this.node.getNodeMetrics()).thenReturn(new NodeMetrics(true));
        // mock send empty entries
        mockSendEmptyEntries();

        this.id = Replicator.start(this.opts, this.raftOptions);
    }

    private void mockSendEmptyEntries() {
        this.mockSendEmptyEntries(false);
    }

    private void mockSendEmptyEntries(final boolean isHeartbeat) {
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest(isHeartbeat);
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequest() {
        return this.createEmptyEntriesRequest(false);
    }

    private RpcRequests.AppendEntriesRequest createEmptyEntriesRequest(final boolean isHeartbeat) {
        RpcRequests.AppendEntriesRequest.Builder rb = RpcRequests.AppendEntriesRequest.newBuilder() //
            .setGroupId("test") //
            .setServerId(new PeerId("localhost", 8082).toString()) //
            .setPeerId(this.peerId.toString()) //
            .setTerm(1) //
            .setPrevLogIndex(10) //
            .setPrevLogTerm(1) //
            .setCommittedIndex(0);
        if (!isHeartbeat) {
            rb.setData(ByteString.EMPTY);
        }
        return rb.build();
    }

    @After
    public void teardown() {
        this.timerManager.shutdown();
    }

    @Test
    public void testStartDestroyJoin() throws Exception {
        assertNotNull(this.id);
        final Replicator r = getReplicator();
        assertNotNull(r);
        assertNotNull(r.getRpcInFly());
        assertEquals(r.statInfo.runningState, Replicator.RunningState.APPENDING_ENTRIES);
        assertSame(r.getOpts(), this.opts);
        this.id.unlock();
        assertEquals(0, Replicator.getNextIndex(this.id));
        assertNotNull(r.getHeartbeatTimer());
        r.destroy();
        Replicator.join(this.id);
        assertNull(r.id);
    }

    @Test
    public void testMetricRemoveOnDestroy() {
        assertNotNull(this.id);
        final Replicator r = getReplicator();
        assertNotNull(r);
        assertSame(r.getOpts(), this.opts);
        Set<String> metrics = this.opts.getNode().getNodeMetrics().getMetricRegistry().getNames();
        assertEquals(6, metrics.size());
        r.destroy();
        metrics = this.opts.getNode().getNodeMetrics().getMetricRegistry().getNames();
        assertEquals(1, metrics.size());
    }

    private Replicator getReplicator() {
        return (Replicator) this.id.lock();
    }

    @Test
    public void testOnRpcReturnedRpcError() {
        testRpcReturnedError();
    }

    private Replicator testRpcReturnedError() {
        final Replicator r = getReplicator();
        assertNull(r.getBlockTimer());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(false) //
            .setLastLogIndex(12) //
            .setTerm(2) //
            .build();
        this.id.unlock();

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, new Status(-1, "test error"), request,
            response, 0, 0, Utils.monotonicMs());
        assertEquals(r.statInfo.runningState, Replicator.RunningState.BLOCKING);
        assertNotNull(r.getBlockTimer());
        return r;
    }

    @Test
    public void testOnRpcReturnedRpcContinuousError() throws Exception {
        Replicator r = testRpcReturnedError();
        ScheduledFuture<?> timer = r.getBlockTimer();
        assertNotNull(timer);

        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(false) //
            .setLastLogIndex(12) //
            .setTerm(2) //
            .build();
        r.getInflights().add(new Replicator.Inflight(RequestType.AppendEntries, r.getNextSendIndex(), 0, 0, 1, null));
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, new Status(-1, "test error"), request,
            response, 1, 1, Utils.monotonicMs());
        assertEquals(r.statInfo.runningState, Replicator.RunningState.BLOCKING);
        assertNotNull(r.getBlockTimer());
        // the same timer
        assertSame(timer, r.getBlockTimer());

        Thread.sleep(r.getOpts().getDynamicHeartBeatTimeoutMs() * 2);
        r.getInflights().add(new Replicator.Inflight(RequestType.AppendEntries, r.getNextSendIndex(), 0, 0, 1, null));
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, new Status(-1, "test error"), request,
            response, 1, 2, Utils.monotonicMs());
        assertEquals(r.statInfo.runningState, Replicator.RunningState.BLOCKING);
        assertNotNull(r.getBlockTimer());
        // the same timer
        assertNotSame(timer, r.getBlockTimer());
    }

    @Test
    public void testOnRpcReturnedTermMismatch() {
        final Replicator r = getReplicator();
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(false) //
            .setLastLogIndex(12) //
            .setTerm(2) //
            .build();
        this.id.unlock();

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());
        Mockito.verify(this.node).increaseTermTo(
            2,
            new Status(RaftError.EHIGHERTERMRESPONSE, "Leader receives higher term heartbeat_response from peer:%s",
                this.peerId));
        assertNull(r.id);
    }

    @Test
    public void testOnRpcReturnedMoreLogs() {
        final Replicator r = getReplicator();
        assertEquals(11, r.getRealNextIndex());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(false) //
            .setLastLogIndex(12) //
            .setTerm(1) //
            .build();
        this.id.unlock();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);

        Mockito.when(this.logManager.getTerm(9)).thenReturn(1L);
        final RpcRequests.AppendEntriesRequest newReq = RpcRequests.AppendEntriesRequest.newBuilder(). //
            setGroupId("test"). //
            setServerId(new PeerId("localhost", 8082).toString()). //
            setPeerId(this.peerId.toString()). //
            setTerm(1). //
            setPrevLogIndex(9). //
            setData(ByteString.EMPTY). //
            setPrevLogTerm(1). //
            setCommittedIndex(0).build();
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(newReq), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());

        assertNotNull(r.getRpcInFly());
        assertNotSame(r.getRpcInFly(), rpcInFly);
        assertEquals(r.statInfo.runningState, Replicator.RunningState.APPENDING_ENTRIES);
        this.id.unlock();
        assertEquals(0, Replicator.getNextIndex(this.id));
        assertEquals(10, r.getRealNextIndex());
    }

    @Test
    public void testOnRpcReturnedLessLogs() {
        final Replicator r = getReplicator();
        assertEquals(11, r.getRealNextIndex());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(false) //
            .setLastLogIndex(8) //
            .setTerm(1) //
            .build();
        this.id.unlock();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);

        Mockito.when(this.logManager.getTerm(8)).thenReturn(1L);
        final RpcRequests.AppendEntriesRequest newReq = RpcRequests.AppendEntriesRequest.newBuilder() //
            .setGroupId("test") //
            .setServerId(new PeerId("localhost", 8082).toString()) //
            .setPeerId(this.peerId.toString()) //
            .setTerm(1) //
            .setPrevLogIndex(8) //
            .setPrevLogTerm(1) //
            .setData(ByteString.EMPTY) //
            .setCommittedIndex(0) //
            .build();
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(newReq), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());

        assertNotNull(r.getRpcInFly());
        assertNotSame(r.getRpcInFly(), rpcInFly);
        assertEquals(r.statInfo.runningState, Replicator.RunningState.APPENDING_ENTRIES);
        this.id.unlock();
        assertEquals(0, Replicator.getNextIndex(this.id));
        assertEquals(9, r.getRealNextIndex());
    }

    @Test
    public void testOnRpcReturnedWaitMoreEntries() throws Exception {
        final Replicator r = getReplicator();
        assertEquals(-1, r.getWaitId());

        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(true) //
            .setLastLogIndex(10) //
            .setTerm(1) //
            .build();
        this.id.unlock();
        Mockito.when(this.logManager.wait(eq(10L), Mockito.any(), same(this.id))).thenReturn(99L);

        final CountDownLatch latch = new CountDownLatch(1);
        Replicator.waitForCaughtUp(this.id, 1, System.currentTimeMillis() + 5000, new CatchUpClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });

        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());

        assertEquals(r.statInfo.runningState, Replicator.RunningState.IDLE);
        this.id.unlock();
        assertEquals(11, Replicator.getNextIndex(this.id));
        assertEquals(99, r.getWaitId());
        latch.await(); //make sure catch up closure is invoked.
    }

    @Test
    public void testStop() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNotNull(r.getHeartbeatTimer());
        assertNotNull(r.getRpcInFly());
        Replicator.stop(this.id);
        assertNull(r.id);
        assertNull(r.getHeartbeatTimer());
        assertNull(r.getRpcInFly());
    }

    @Test
    public void testSetErrorStop() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNotNull(r.getHeartbeatTimer());
        assertNotNull(r.getRpcInFly());
        this.id.setError(RaftError.ESTOP.getNumber());
        this.id.unlock();
        assertNull(r.id);
        assertNull(r.getHeartbeatTimer());
        assertNull(r.getRpcInFly());
    }

    @Test
    public void testContinueSendingTimeout() throws Exception {
        testOnRpcReturnedWaitMoreEntries();
        final Replicator r = getReplicator();
        this.id.unlock();
        mockSendEmptyEntries();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);
        assertTrue(Replicator.continueSending(this.id, RaftError.ETIMEDOUT.getNumber()));
        assertNotNull(r.getRpcInFly());
        assertNotSame(rpcInFly, r.getRpcInFly());
    }

    @Test
    public void testContinueSendingEntries() throws Exception {
        testOnRpcReturnedWaitMoreEntries();
        final Replicator r = getReplicator();
        this.id.unlock();
        mockSendEmptyEntries();
        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);

        final RpcRequests.AppendEntriesRequest.Builder rb = RpcRequests.AppendEntriesRequest.newBuilder() //
            .setGroupId("test") //
            .setServerId(new PeerId("localhost", 8082).toString()) //
            .setPeerId(this.peerId.toString()) //
            .setTerm(1) //
            .setPrevLogIndex(10) //
            .setPrevLogTerm(1) //
            .setCommittedIndex(0);

        int totalDataLen = 0;
        for (int i = 0; i < 10; i++) {
            totalDataLen += i;
            final LogEntry value = new LogEntry();
            value.setData(ByteBuffer.allocate(i));
            value.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            value.setId(new LogId(11 + i, 1));
            Mockito.when(this.logManager.getEntry(11 + i)).thenReturn(value);
            rb.addEntries(RaftOutter.EntryMeta.newBuilder().setTerm(1).setType(EnumOutter.EntryType.ENTRY_TYPE_DATA)
                .setDataLen(i));
        }
        rb.setData(ByteString.copyFrom(new byte[totalDataLen]));

        final RpcRequests.AppendEntriesRequest request = rb.build();
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());

        assertEquals(11, r.statInfo.firstLogIndex);
        assertEquals(10, r.statInfo.lastLogIndex);
        Mockito.when(this.logManager.getTerm(20)).thenReturn(1L);
        assertTrue(Replicator.continueSending(this.id, 0));
        assertNotNull(r.getRpcInFly());
        assertNotSame(rpcInFly, r.getRpcInFly());
        assertEquals(11, r.statInfo.firstLogIndex);
        assertEquals(20, r.statInfo.lastLogIndex);
        assertEquals(0, r.getWaitId());
        assertEquals(r.statInfo.runningState, Replicator.RunningState.IDLE);
    }

    @Test
    public void testSetErrorTimeout() throws Exception {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getHeartbeatInFly());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest(true);
        Mockito.when(
            this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request),
                eq(this.opts.getElectionTimeoutMs() / 2), Mockito.any())).thenReturn(new FutureImpl<>());
        this.id.setError(RaftError.ETIMEDOUT.getNumber());
        Thread.sleep(this.opts.getElectionTimeoutMs() + 1000);
        assertNotNull(r.getHeartbeatInFly());
    }

    @Test
    public void testOnHeartbeatReturnedRpcError() {
        final Replicator r = getReplicator();
        this.id.unlock();
        final ScheduledFuture<?> timer = r.getHeartbeatTimer();
        assertNotNull(timer);
        Replicator.onHeartbeatReturned(this.id, new Status(-1, "test"), createEmptyEntriesRequest(), null,
            Utils.monotonicMs());
        assertNotNull(r.getHeartbeatTimer());
        assertNotSame(timer, r.getHeartbeatTimer());
    }

    @Test
    public void testOnHeartbeatReturnedOK() {
        final Replicator r = getReplicator();
        this.id.unlock();
        final ScheduledFuture<?> timer = r.getHeartbeatTimer();
        assertNotNull(timer);
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder(). //
            setSuccess(false). //
            setLastLogIndex(10).setTerm(1).build();
        Replicator
            .onHeartbeatReturned(this.id, Status.OK(), createEmptyEntriesRequest(), response, Utils.monotonicMs());
        assertNotNull(r.getHeartbeatTimer());
        assertNotSame(timer, r.getHeartbeatTimer());
    }

    @Test
    public void testOnHeartbeatReturnedTermMismatch() {
        final Replicator r = getReplicator();
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder() //
            .setSuccess(false) //
            .setLastLogIndex(12) //
            .setTerm(2) //
            .build();
        this.id.unlock();

        Replicator.onHeartbeatReturned(this.id, Status.OK(), request, response, Utils.monotonicMs());
        Mockito.verify(this.node).increaseTermTo(
            2,
            new Status(RaftError.EHIGHERTERMRESPONSE, "Leader receives higher term heartbeat_response from peer:%s",
                this.peerId));
        assertNull(r.id);
    }

    @Test
    public void testTransferLeadership() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertEquals(0, r.getTimeoutNowIndex());
        assertTrue(Replicator.transferLeadership(this.id, 11));
        assertEquals(11, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
    }

    @Test
    public void testStopTransferLeadership() {
        testTransferLeadership();
        Replicator.stopTransferLeadership(this.id);
        final Replicator r = getReplicator();
        this.id.unlock();
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
    }

    @Test
    public void testTransferLeadershipSendTimeoutNow() {
        final Replicator r = getReplicator();
        this.id.unlock();
        r.setHasSucceeded();
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());

        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        Mockito.when(
            this.rpcService.timeoutNow(Matchers.eq(this.opts.getPeerId().getEndpoint()), eq(request), eq(-1),
                Mockito.any())).thenReturn(new FutureImpl<>());

        assertTrue(Replicator.transferLeadership(this.id, 10));
        assertEquals(0, r.getTimeoutNowIndex());
        assertNotNull(r.getTimeoutNowInFly());
    }

    @Test
    public void testSendHeartbeat() {
        final Replicator r = getReplicator();
        this.id.unlock();

        assertNull(r.getHeartbeatInFly());
        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest(true);
        Mockito.when(
            this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request),
                eq(this.opts.getElectionTimeoutMs() / 2), Mockito.any())).thenReturn(new FutureImpl<>());
        Replicator.sendHeartbeat(this.id, new RpcResponseClosureAdapter<RpcRequests.AppendEntriesResponse>() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());

            }
        });

        assertNotNull(r.getHeartbeatInFly());

        assertSame(r, this.id.lock());
        this.id.unlock();
    }

    @Test
    public void testSendTimeoutNowAndStop() {
        final Replicator r = getReplicator();
        this.id.unlock();
        r.setHasSucceeded();
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
        assertTrue(Replicator.sendTimeoutNowAndStop(this.id, 10));
        assertEquals(0, r.getTimeoutNowIndex());
        assertNull(r.getTimeoutNowInFly());
        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        Mockito.verify(this.rpcService).timeoutNow(Matchers.eq(this.opts.getPeerId().getEndpoint()), eq(request),
            eq(10), Mockito.any());
    }

    private RpcRequests.TimeoutNowRequest createTimeoutnowRequest() {
        final RpcRequests.TimeoutNowRequest.Builder rb = RpcRequests.TimeoutNowRequest.newBuilder();
        rb.setTerm(this.opts.getTerm());
        rb.setGroupId(this.opts.getGroupId());
        rb.setServerId(this.opts.getServerId().toString());
        rb.setPeerId(this.opts.getPeerId().toString());
        return rb.build();
    }

    @Test
    public void testOnTimeoutNowReturnedRpcErrorAndStop() {
        final Replicator r = getReplicator();
        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        this.id.unlock();

        Replicator.onTimeoutNowReturned(this.id, new Status(-1, "test"), request, null, true);
        assertNull(r.id);
    }

    @Test
    public void testInstallSnapshotNoReader() {
        final Replicator r = getReplicator();
        this.id.unlock();

        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);
        r.installSnapshot();
        final ArgumentCaptor<RaftException> errArg = ArgumentCaptor.forClass(RaftException.class);
        Mockito.verify(this.node).onError(errArg.capture());
        Assert.assertEquals(RaftError.EIO, errArg.getValue().getStatus().getRaftError());
        Assert.assertEquals("Fail to open snapshot", errArg.getValue().getStatus().getErrorMsg());
    }

    @Test
    public void testInstallSnapshot() {
        final Replicator r = getReplicator();
        this.id.unlock();

        final Future<Message> rpcInFly = r.getRpcInFly();
        assertNotNull(rpcInFly);
        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);
        Mockito.when(this.snapshotStorage.open()).thenReturn(reader);
        final String uri = "remote://localhost:8081/99";
        Mockito.when(reader.generateURIForCopy()).thenReturn(uri);
        final RaftOutter.SnapshotMeta meta = RaftOutter.SnapshotMeta.newBuilder() //
            .setLastIncludedIndex(11) //
            .setLastIncludedTerm(1) //
            .build();
        Mockito.when(reader.load()).thenReturn(meta);

        assertEquals(0, r.statInfo.lastLogIncluded);
        assertEquals(0, r.statInfo.lastTermIncluded);

        final RpcRequests.InstallSnapshotRequest.Builder rb = RpcRequests.InstallSnapshotRequest.newBuilder();
        rb.setTerm(this.opts.getTerm());
        rb.setGroupId(this.opts.getGroupId());
        rb.setServerId(this.opts.getServerId().toString());
        rb.setPeerId(this.opts.getPeerId().toString());
        rb.setMeta(meta);
        rb.setUri(uri);

        Mockito.when(
            this.rpcService.installSnapshot(Matchers.eq(this.opts.getPeerId().getEndpoint()), eq(rb.build()),
                Mockito.any())).thenReturn(new FutureImpl<>());

        r.installSnapshot();
        assertNotNull(r.getRpcInFly());
        assertNotSame(r.getRpcInFly(), rpcInFly);
        Assert.assertEquals(Replicator.RunningState.INSTALLING_SNAPSHOT, r.statInfo.runningState);
        assertEquals(11, r.statInfo.lastLogIncluded);
        assertEquals(1, r.statInfo.lastTermIncluded);
    }

    @Test
    public void testOnTimeoutNowReturnedTermMismatch() {
        final Replicator r = getReplicator();
        this.id.unlock();
        final RpcRequests.TimeoutNowRequest request = createTimeoutnowRequest();
        final RpcRequests.TimeoutNowResponse response = RpcRequests.TimeoutNowResponse.newBuilder() //
            .setSuccess(false) //
            .setTerm(12) //
            .build();
        this.id.unlock();

        Replicator.onTimeoutNowReturned(this.id, Status.OK(), request, response, false);
        Mockito.verify(this.node).increaseTermTo(
            12,
            new Status(RaftError.EHIGHERTERMRESPONSE, "Leader receives higher term timeout_now_response from peer:%s",
                this.peerId));
        assertNull(r.id);
    }

    @Test
    public void testOnInstallSnapshotReturned() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getBlockTimer());

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = RpcRequests.InstallSnapshotResponse.newBuilder()
            .setSuccess(true).setTerm(1).build();
        assertEquals(-1, r.getWaitId());
        Mockito.when(this.logManager.getTerm(11)).thenReturn(1L);
        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, Status.OK(), request, response, 0, 0, -1);
        assertNull(r.getBlockTimer());
        assertEquals(0, r.getWaitId());
    }

    @Test
    public void testOnInstallSnapshotReturnedRpcError() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getBlockTimer());

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = RpcRequests.InstallSnapshotResponse.newBuilder()
            .setSuccess(true).setTerm(1).build();
        assertEquals(-1, r.getWaitId());
        Mockito.when(this.logManager.getTerm(11)).thenReturn(1L);
        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, new Status(-1, "test"), request, response,
            0, 0, -1);
        assertNotNull(r.getBlockTimer());
        assertEquals(-1, r.getWaitId());
    }

    @Test
    public void testOnInstallSnapshotReturnedFailure() {
        final Replicator r = getReplicator();
        this.id.unlock();
        assertNull(r.getBlockTimer());

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = RpcRequests.InstallSnapshotResponse.newBuilder()
            .setSuccess(false).setTerm(1).build();
        assertEquals(-1, r.getWaitId());
        Mockito.when(this.logManager.getTerm(11)).thenReturn(1L);
        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, Status.OK(), request, response, 0, 0, -1);
        assertNotNull(r.getBlockTimer());
        assertEquals(-1, r.getWaitId());
    }

    @Test
    public void testRetryInstallSnapshotUnderSameVersion() {
        final Replicator r = getReplicator();
        this.id.unlock();

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = RpcRequests.InstallSnapshotResponse.newBuilder()
            .setSuccess(false).setTerm(1).build();

        final SnapshotReader reader = Mockito.mock(SnapshotReader.class);
        Mockito.when(this.snapshotStorage.open()).thenReturn(reader, null);
        final String uri = "remote://localhost:8081/99";
        Mockito.when(reader.generateURIForCopy()).thenReturn(uri);

        final RaftOutter.SnapshotMeta meta = RaftOutter.SnapshotMeta.newBuilder() //
            .setLastIncludedIndex(11) //
            .setLastIncludedTerm(1) //
            .build();
        Mockito.when(reader.load()).thenReturn(meta);

        Mockito.when(this.rpcService.installSnapshot(Matchers.eq(this.opts.getPeerId().getEndpoint()), any(), any()))
            .thenReturn(new FutureImpl<>());

        r.installSnapshot();
        assertEquals(reader, r.getReader());

        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, new Status(RaftError.ECANCELED, "cancel"),
            request, response, 0, 0, -1);
        r.installSnapshot();
        // two installSnapshot call get same reader
        assertEquals(reader, r.getReader());
    }

    @Test
    public void testRetryInstallSnapshotUnderDiffVersion() {
        final Replicator r = getReplicator();
        this.id.unlock();

        final RpcRequests.InstallSnapshotRequest request = createInstallSnapshotRequest();
        final RpcRequests.InstallSnapshotResponse response = RpcRequests.InstallSnapshotResponse.newBuilder()
            .setSuccess(false).setTerm(1).build();

        final SnapshotReader readerOld = Mockito.mock(SnapshotReader.class);
        final SnapshotReader readerNew = Mockito.mock(SnapshotReader.class);
        Mockito.when(this.snapshotStorage.open()).thenReturn(readerOld, readerNew, null);
        final String uriOld = "remote://localhost:8081/99";
        Mockito.when(readerOld.generateURIForCopy()).thenReturn(uriOld);

        final RaftOutter.SnapshotMeta meta = RaftOutter.SnapshotMeta.newBuilder() //
            .setLastIncludedIndex(11) //
            .setLastIncludedTerm(1) //
            .build();
        Mockito.when(readerOld.load()).thenReturn(meta);

        final String uriNew = "remote://localhost:8081/100";
        Mockito.when(readerNew.generateURIForCopy()).thenReturn(uriNew);

        final RaftOutter.SnapshotMeta metaNew = RaftOutter.SnapshotMeta.newBuilder() //
            .setLastIncludedIndex(12) //
            .setLastIncludedTerm(1) //
            .build();
        Mockito.when(readerNew.load()).thenReturn(metaNew);

        Mockito.when(this.rpcService.installSnapshot(Matchers.eq(this.opts.getPeerId().getEndpoint()), any(), any()))
            .thenReturn(new FutureImpl<>());

        r.installSnapshot();
        assertEquals(readerOld, r.getReader());

        Replicator.onRpcReturned(this.id, Replicator.RequestType.Snapshot, new Status(RaftError.ECANCELED, "cancel"),
            request, response, 0, 0, -1);

        r.installSnapshot();
        // two installSnapshot call get same reader
        assertEquals(readerNew, r.getReader());
    }

    @Test
    public void testOnRpcReturnedOutOfOrder() {
        final Replicator r = getReplicator();
        assertEquals(-1, r.getWaitId());

        final RpcRequests.AppendEntriesRequest request = createEmptyEntriesRequest();
        final RpcRequests.AppendEntriesResponse response = RpcRequests.AppendEntriesResponse.newBuilder(). //
            setSuccess(true). //
            setLastLogIndex(10).setTerm(1).build();
        assertNull(r.getBlockTimer());
        this.id.unlock();

        assertTrue(r.getPendingResponses().isEmpty());
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 1, 0,
            Utils.monotonicMs());
        assertEquals(1, r.getPendingResponses().size());
        Replicator.onRpcReturned(this.id, Replicator.RequestType.AppendEntries, Status.OK(), request, response, 0, 0,
            Utils.monotonicMs());
        assertTrue(r.getPendingResponses().isEmpty());
        assertEquals(0, r.getWaitId());
        assertEquals(11, r.getRealNextIndex());
        assertEquals(1, r.getRequiredNextSeq());
    }

    private void mockSendEntries(@SuppressWarnings("SameParameterValue") final int n) {
        final RpcRequests.AppendEntriesRequest request = createEntriesRequest(n);
        Mockito.when(this.rpcService.appendEntries(eq(this.peerId.getEndpoint()), eq(request), eq(-1), Mockito.any()))
            .thenReturn(new FutureImpl<>());
    }

    private RpcRequests.AppendEntriesRequest createEntriesRequest(final int n) {
        final RpcRequests.AppendEntriesRequest.Builder rb = RpcRequests.AppendEntriesRequest.newBuilder() //
            .setGroupId("test") //
            .setServerId(new PeerId("localhost", 8082).toString()) //
            .setPeerId(this.peerId.toString()) //
            .setTerm(1) //
            .setPrevLogIndex(10) //
            .setPrevLogTerm(1) //
            .setCommittedIndex(0);

        for (int i = 0; i < n; i++) {
            final LogEntry log = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_DATA);
            log.setData(ByteBuffer.wrap(new byte[i]));
            log.setId(new LogId(i + 11, 1));
            Mockito.when(this.logManager.getEntry(i + 11)).thenReturn(log);
            Mockito.when(this.logManager.getTerm(i + 11)).thenReturn(1L);
            rb.addEntries(RaftOutter.EntryMeta.newBuilder().setDataLen(i).setTerm(1)
                .setType(EnumOutter.EntryType.ENTRY_TYPE_DATA).build());
        }

        return rb.build();
    }

    @Test
    public void testGetNextSendIndex() {
        final Replicator r = getReplicator();
        assertEquals(-1, r.getNextSendIndex());
        r.resetInflights();
        assertEquals(11, r.getNextSendIndex());
        mockSendEntries(3);
        r.sendEntries();
        assertEquals(14, r.getNextSendIndex());
    }

    private RpcRequests.InstallSnapshotRequest createInstallSnapshotRequest() {
        final String uri = "remote://localhost:8081/99";
        final RaftOutter.SnapshotMeta meta = RaftOutter.SnapshotMeta.newBuilder() //
            .setLastIncludedIndex(11) //
            .setLastIncludedTerm(1) //
            .build();
        final RpcRequests.InstallSnapshotRequest.Builder rb = RpcRequests.InstallSnapshotRequest.newBuilder();
        rb.setTerm(this.opts.getTerm());
        rb.setGroupId(this.opts.getGroupId());
        rb.setServerId(this.opts.getServerId().toString());
        rb.setPeerId(this.opts.getPeerId().toString());
        rb.setMeta(meta);
        rb.setUri(uri);
        return rb.build();
    }
}
