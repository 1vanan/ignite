/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.datastreamer;

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteDataStreamerTimeoutException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessor;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.IgniteCacheFutureImpl;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.stream.StreamReceiver;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_DATASTREAM;

/**
 * Data streamer implementation.
 */
@SuppressWarnings("unchecked")
public class DataStreamerImpl<K, V> implements IgniteDataStreamer<K, V>, Delayed {
    /** Buffer size for streaming per batch. */
    private int bufLdrSizePerBatch = DFLT_LOADER_SIZE;

    /** Thread buffer map. */
    private volatile Map<Long, IgniteBiTuple<IgniteCacheFutureImpl, List<DataStreamerEntry>>> threadBufMap =
            new ConcurrentHashMap8<>();

    /** Closed streamer. */
    boolean closedStreamer = false;

    /** Streamer lock. */
    ReadWriteLock streamerLock = new ReentrantReadWriteLock();

    /** Isolated receiver. */
    private static final StreamReceiver ISOLATED_UPDATER = new IsolatedUpdater();

    /** Amount of permissions should be available to continue new data processing. */
    private static final int REMAP_SEMAPHORE_PERMISSIONS_COUNT = Integer.MAX_VALUE;

    /** Cache receiver. */
    private StreamReceiver<K, V> rcvr = ISOLATED_UPDATER;

    /** */
    private byte[] updaterBytes;

    /** IO policy resovler for data load request. */
    private IgniteClosure<ClusterNode, Byte> ioPlcRslvr;

    /** Max remap count before issuing an error. */
    private static final int DFLT_MAX_REMAP_CNT = 32;

    /** Log reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Cache name ({@code null} for default cache). */
    private final String cacheName;

    /** Per-node buffer size. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private int bufSize = DFLT_PER_NODE_BUFFER_SIZE;

    /** */
    private int parallelOps;

    /** */
    private long timeout = DFLT_UNLIMIT_TIMEOUT;

    /** */
    private long autoFlushFreq;

    /** Mapping. */
    @GridToStringInclude
    private ConcurrentMap<UUID, Buffer> bufMappings = new ConcurrentHashMap8<>();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr;

    /** Context. */
    private final GridKernalContext ctx;

    /** */
    private final IgniteCacheObjectProcessor cacheObjProc;

    /** */
    private final CacheObjectContext cacheObjCtx;

    /** Communication topic for responses. */
    private final Object topic;

    /** */
    private byte[] topicBytes;

    /** {@code True} if data loader has been cancelled. */
    private volatile boolean cancelled;

    /** Cancellation reason. */
    private volatile Throwable cancellationReason = null;

    /** Fail counter. */
    private final LongAdder failCntr = new LongAdder();

    /** Active futures of this data loader. */
    @GridToStringInclude
    private final Collection<IgniteInternalFuture<?>> activeFuts = new GridConcurrentHashSet<>();

    /** Closure to remove from active futures. */
    @GridToStringExclude
    private final IgniteInClosure<IgniteInternalFuture<?>> rmvActiveFut = new IgniteInClosure<IgniteInternalFuture<?>>() {
        @Override public void apply(IgniteInternalFuture<?> t) {
            boolean rmv = activeFuts.remove(t);

            assert rmv;

            Throwable err = t.error();

            if (err != null && !(err instanceof IgniteClientDisconnectedCheckedException)) {
                LT.error(log, t.error(), "DataStreamer operation failed.", true);

                failCntr.increment();

                synchronized (DataStreamerImpl.this) {
                    if(cancellationReason == null)
                        cancellationReason = err;

                    cancelled = true;
                }
            }
        }
    };

    /** Job peer deploy aware. */
    private volatile GridPeerDeployAware jobPda;

    /** Deployment class. */
    private Class<?> depCls;

    /** Future to track loading finish. */
    private final GridFutureAdapter<?> fut;

    /** Public API future to track loading finish. */
    private final IgniteFuture<?> publicFut;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** */
    private CacheException disconnectErr;

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** */
    private volatile long lastFlushTime = U.currentTimeMillis();

    /** */
    private final DelayQueue<DataStreamerImpl<K, V>> flushQ;

    /** */
    private boolean skipStore;

    /** */
    private boolean keepBinary;

    /** */
    private int maxRemapCnt = DFLT_MAX_REMAP_CNT;

    /** Whether a warning at {@link DataStreamerImpl#allowOverwrite()} printed */
    private static boolean isWarningPrinted;

    /** Allows to pause new data processing while failed data processing in progress. */
    private final Semaphore remapSem = new Semaphore(REMAP_SEMAPHORE_PERMISSIONS_COUNT);

    /** */
    private final ConcurrentLinkedDeque<Runnable> dataToRemap = new ConcurrentLinkedDeque<>();

    /** */
    private final AtomicBoolean remapOwning = new AtomicBoolean();

    /**
     * @param ctx Grid kernal context.
     * @param cacheName Cache name.
     * @param flushQ Flush queue.
     */
    public DataStreamerImpl(
        final GridKernalContext ctx,
        @Nullable final String cacheName,
        DelayQueue<DataStreamerImpl<K, V>> flushQ
    ) {
        assert ctx != null;

        this.ctx = ctx;
        this.cacheObjProc = ctx.cacheObjects();

        if (log == null)
            log = U.logger(ctx, logRef, DataStreamerImpl.class);

        CacheConfiguration ccfg = ctx.cache().cacheConfiguration(cacheName);

        try {
            this.cacheObjCtx = ctx.cacheObjects().contextForCache(ccfg);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to initialize cache context.", e);
        }

        this.cacheName = cacheName;
        this.flushQ = flushQ;

        discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                UUID id = discoEvt.eventNode().id();

                // Remap regular mappings.
                final Buffer buf = bufMappings.remove(id);

                // Only async notification is possible since
                // discovery thread may be trapped otherwise.
                if (buf != null) {
                    waitAffinityAndRun(new Runnable() {
                        @Override public void run() {
                            buf.onNodeLeft();
                        }
                    }, discoEvt.topologyVersion(), true);
                }
            }
        };

        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

        // Generate unique topic for this loader.
        topic = TOPIC_DATASTREAM.topic(IgniteUuid.fromUuid(ctx.localNodeId()));

        ctx.io().addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                assert msg instanceof DataStreamerResponse;

                DataStreamerResponse res = (DataStreamerResponse)msg;

                if (log.isDebugEnabled())
                    log.debug("Received data load response: " + res);

                Buffer buf = bufMappings.get(nodeId);

                if (buf != null)
                    buf.onResponse(res, nodeId);

                else if (log.isDebugEnabled())
                    log.debug("Ignoring response since node has left [nodeId=" + nodeId + ", ");
            }
        });

        if (log.isDebugEnabled())
            log.debug("Added response listener within topic: " + topic);

        fut = new DataStreamerFuture(this);

        publicFut = new IgniteCacheFutureImpl<>(fut);

        GridCacheAdapter cache = ctx.cache().internalCache(cacheName);

        if (cache == null) { // Possible, cache is not configured on node.
            assert ccfg != null;

            if (ccfg.getCacheMode() == CacheMode.LOCAL)
                throw new CacheException("Impossible to load Local cache configured remotely.");

            ctx.grid().getOrCreateCache(ccfg);
        }

        ensureCacheStarted();
    }

    /** {@inheritDoc} */
    public void perBatchBufferSize(int size) {
        this.bufLdrSizePerBatch = size;
    }

    /** {@inheritDoc} */
    public int perBatchBufferSize() {
        return this.bufLdrSizePerBatch;
    }

    /**
     * @param c Closure to run.
     * @param topVer Topology version to wait for.
     * @param async Async flag.
     */
    private void waitAffinityAndRun(final Runnable c, long topVer, boolean async) {
        AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer, 0);

        IgniteInternalFuture<?> fut = ctx.cache().context().exchange().affinityReadyFuture(topVer0);

        if (fut != null && !fut.isDone()) {
            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> fut) {
                    ctx.closure().runLocalSafe(c, true);
                }
            });
        }
        else {
            if (async)
                ctx.closure().runLocalSafe(c, true);
            else
                c.run();
        }
    }

    /**
     * @return Cache object context.
     */
    public CacheObjectContext cacheObjectContext() {
        return cacheObjCtx;
    }

    /**
     * Enters busy lock.
     */
    private void enterBusy() {
        if (!busyLock.enterBusy()) {
            if (disconnectErr != null)
                throw disconnectErr;

            closedException();
        }
        else if (cancelled) {
            busyLock.leaveBusy();

            closedException();
        }
    }

    /**
     * Leaves busy lock.
     */
    private void leaveBusy() {
        busyLock.leaveBusy();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> future() {
        return publicFut;
    }

    /**
     * @return Internal future.
     */
    public IgniteInternalFuture<?> internalFuture() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public void deployClass(Class<?> depCls) {
        this.depCls = depCls;
    }

    /** {@inheritDoc} */
    @Override public void receiver(StreamReceiver<K, V> rcvr) {
        A.notNull(rcvr, "rcvr");

        this.rcvr = rcvr;
    }

    /** {@inheritDoc} */
    @Override public boolean allowOverwrite() {
        return rcvr != ISOLATED_UPDATER;
    }

    /** {@inheritDoc} */
    @Override public void allowOverwrite(boolean allow) {
        if (allow == allowOverwrite())
            return;

        ClusterNode node = F.first(ctx.grid().cluster().forCacheNodes(cacheName).nodes());

        if (node == null)
            throw new CacheException("Failed to get node for cache: " + cacheName);

        rcvr = allow ? DataStreamerCacheUpdaters.<K, V>individual() : ISOLATED_UPDATER;
    }

    /** {@inheritDoc} */
    @Override public boolean skipStore() {
        return skipStore;
    }

    /** {@inheritDoc} */
    @Override public void skipStore(boolean skipStore) {
        this.skipStore = skipStore;
    }

    /** {@inheritDoc} */
    @Override public boolean keepBinary() {
        return keepBinary;
    }

    /** {@inheritDoc} */
    @Override public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc} */
    @Override @Nullable public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public int perNodeBufferSize() {
        return bufSize;
    }

    /** {@inheritDoc} */
    @Override public void perNodeBufferSize(int bufSize) {
        A.ensure(bufSize > 0, "bufSize > 0");

        this.bufSize = bufSize;
    }

    /** {@inheritDoc} */
    @Override public int perNodeParallelOperations() {
        return parallelOps;
    }

    /** {@inheritDoc} */
    @Override public void perNodeParallelOperations(int parallelOps) {
        this.parallelOps = parallelOps;
    }

    /** {@inheritDoc} */
    @Override public void timeout(long timeout) {
        if (timeout < -1 || timeout == 0)
            throw new IllegalArgumentException();

        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return this.timeout;
    }

    /** {@inheritDoc} */
    @Override public long autoFlushFrequency() {
        return autoFlushFreq;
    }

    /** {@inheritDoc} */
    @Override public void autoFlushFrequency(long autoFlushFreq) {
        A.ensure(autoFlushFreq >= 0, "autoFlushFreq >= 0");

        long old = this.autoFlushFreq;

        if (autoFlushFreq != old) {
            this.autoFlushFreq = autoFlushFreq;

            if (autoFlushFreq != 0 && old == 0)
                flushQ.add(this);
            else if (autoFlushFreq == 0)
                flushQ.remove(this);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(Map<K, V> entries) throws IllegalStateException {
        A.notNull(entries, "entries");

        return addData(entries.entrySet());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(Collection<? extends Map.Entry<K, V>> entries) {
        A.notEmpty(entries, "entries");

        checkSecurityPermission(SecurityPermission.CACHE_PUT);

        enterBusy();

        try {
            GridFutureAdapter<Object> resFut = new GridFutureAdapter<>();

            resFut.listen(rmvActiveFut);

            activeFuts.add(resFut);

            Collection<KeyCacheObjectWrapper> keys =
                new GridConcurrentHashSet<>(entries.size(), U.capacity(entries.size()), 1);

            Collection<DataStreamerEntry> entries0 = new ArrayList<>(entries.size());

            for (Map.Entry<K, V> entry : entries) {
                KeyCacheObject key = cacheObjProc.toCacheKeyObject(cacheObjCtx, null, entry.getKey(), true);
                CacheObject val = cacheObjProc.toCacheObject(cacheObjCtx, entry.getValue(), true);

                keys.add(new KeyCacheObjectWrapper(key));

                entries0.add(new DataStreamerEntry(key, val));
            }

            load0(entries0, resFut, keys, 0);

            return new IgniteCacheFutureImpl<>(resFut);
        }
        catch (IgniteDataStreamerTimeoutException e) {
            throw e;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return Future.
     */
    public IgniteFuture<?> addDataInternal(KeyCacheObject key, CacheObject val) {
        return addDataInternal(Collections.singleton(new DataStreamerEntry(key, val)));
    }

    /**
     * @param key Key.
     * @return Future.
     */
    public IgniteFuture<?> removeDataInternal(KeyCacheObject key) {
        return addDataInternal(Collections.singleton(new DataStreamerEntry(key, null)));
    }

    /**
     * Clear buffer collections in case of streaming data per batch.
     */
    private void clearBuf() {
        threadBufMap.get(Thread.currentThread().getId()).get2().clear();

        threadBufMap.get(Thread.currentThread().getId()).set1(new IgniteCacheFutureImpl(new GridFutureAdapter()));
    }

    /**
     * @param entries Entries.
     * @return Future.
     */
    public IgniteFuture<?> addDataInternal(Collection<? extends DataStreamerEntry> entries) {
        enterBusy();

        GridFutureAdapter<Object> resFut = new GridFutureAdapter<>();

        try {
            resFut.listen(rmvActiveFut);

            activeFuts.add(resFut);

            Collection<KeyCacheObjectWrapper> keys = null;

            if (entries.size() > 1) {
                keys = new GridConcurrentHashSet<>(entries.size(), U.capacity(entries.size()), 1);

                for (DataStreamerEntry entry : entries)
                    keys.add(new KeyCacheObjectWrapper(entry.getKey()));
            }

            load0(entries, resFut, keys, 0);

            return new IgniteCacheFutureImpl<>(resFut);
        }
        catch (Throwable e) {
            resFut.onDone(e);

            if (e instanceof Error || e instanceof IgniteDataStreamerTimeoutException)
                throw e;

            return new IgniteCacheFutureImpl<>(resFut);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param entry Entry.
     * @return Future.
     */
    private IgniteFuture<?> addDataInternal(DataStreamerEntry entry) {
        enterBusy();

        streamerLock.readLock().lock();

        Collection<DataStreamerEntry> entries0;

        IgniteCacheFutureImpl fut0;

        if (!threadBufMap.containsKey(Thread.currentThread().getId())) {
            IgniteCacheFutureImpl futPerBatch = new IgniteCacheFutureImpl(new GridFutureAdapter());

            List<DataStreamerEntry> entriesPerBatch = new ArrayList<>(bufLdrSizePerBatch);

            IgniteBiTuple<IgniteCacheFutureImpl, List<DataStreamerEntry>> futBufPair =
                    new IgniteBiTuple(futPerBatch, entriesPerBatch);

            threadBufMap.put(Thread.currentThread().getId(), futBufPair);
        }

        fut0 = threadBufMap.get(Thread.currentThread().getId()).get1();

        try {
            threadBufMap.get(Thread.currentThread().getId()).get2().add(entry);

            entries0 = threadBufMap.get(Thread.currentThread().getId()).get2();

            if (!activeFuts.contains(fut0.internalFuture())) {
                activeFuts.add(fut0.internalFuture());

                fut0.internalFuture().listen(rmvActiveFut);
            }

            if (entries0.size() == bufLdrSizePerBatch) {
                loadData(entries0, fut0);

                clearBuf();
            }

            return fut0;
        }
        catch (Throwable e) {
            GridFutureAdapter internalFut = (GridFutureAdapter) fut0.internalFuture();

            internalFut.onDone(e);

            if (e instanceof Error || e instanceof IgniteDataStreamerTimeoutException)
                throw e;

            return fut0;
        }
        finally {
            leaveBusy();

            streamerLock.readLock().unlock();
        }
    }

    /**
     * Load batch of DataStreamerEntry from buffer loadingDataPerBatch.
     */
    private IgniteCacheFutureImpl loadData(Collection<? extends DataStreamerEntry> entries, IgniteCacheFutureImpl future) {
        Collection<KeyCacheObjectWrapper> keys = null;

        if (entries.size() > 1) {
            keys = new GridConcurrentHashSet<>(entries.size(),
                    U.capacity(entries.size()), 1);

            for (DataStreamerEntry e : entries)
                keys.add(new KeyCacheObjectWrapper(e.getKey()));

        }

        if (entries.size() > 0) {
            load0(entries, (GridFutureAdapter) future.internalFuture(), keys, 0);

            entries.clear();
        }

        return future;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(Map.Entry<K, V> entry) {
        A.notNull(entry, "entry");

        return addData(F.asList(entry));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> addData(K key, V val) {
        if (closedStreamer)
            throw new IllegalStateException("Data streamer has been closed.", cancellationReason);

        A.notNull(key, "key");

        if (val == null)
            checkSecurityPermission(SecurityPermission.CACHE_REMOVE);
        else
            checkSecurityPermission(SecurityPermission.CACHE_PUT);

        KeyCacheObject key0 = cacheObjProc.toCacheKeyObject(cacheObjCtx, null, key, true);
        CacheObject val0 = cacheObjProc.toCacheObject(cacheObjCtx, val, true);

        return addDataInternal(new DataStreamerEntry(key0, val0));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> removeData(K key) {
        return addData(key, null);
    }

    /**
     * @param ioPlcRslvr IO policy resolver.
     */
    public void ioPolicyResolver(IgniteClosure<ClusterNode, Byte> ioPlcRslvr) {
        this.ioPlcRslvr = ioPlcRslvr;
    }

    /**
     *
     */
    private void acquireRemapSemaphore() throws IgniteInterruptedCheckedException {
        try {
            if (remapSem.availablePermits() != REMAP_SEMAPHORE_PERMISSIONS_COUNT) {
                if (timeout == DFLT_UNLIMIT_TIMEOUT) {
                    // Wait until failed data being processed.
                    remapSem.acquire(REMAP_SEMAPHORE_PERMISSIONS_COUNT);

                    remapSem.release(REMAP_SEMAPHORE_PERMISSIONS_COUNT);
                }
                else {
                    // Wait until failed data being processed.
                    boolean res = remapSem.tryAcquire(REMAP_SEMAPHORE_PERMISSIONS_COUNT, timeout, TimeUnit.MILLISECONDS);

                    if (res)
                        remapSem.release(REMAP_SEMAPHORE_PERMISSIONS_COUNT);
                    else
                        throw new IgniteDataStreamerTimeoutException("Data streamer exceeded timeout " +
                            "while was waiting for failed data resending finished.");
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * @param entries Entries.
     * @param resFut Result future.
     * @param activeKeys Active keys.
     * @param remaps Remaps count.
     */
    private void load0(
        Collection<? extends DataStreamerEntry> entries,
        final GridFutureAdapter<Object> resFut,
        @Nullable final Collection<KeyCacheObjectWrapper> activeKeys,
        final int remaps
    ) {
        try {
            assert entries != null;

            final boolean remap = remaps > 0;

            if (!remap) { // Failed data should be processed prior to new data.
                acquireRemapSemaphore();
            }

            if (!isWarningPrinted) {
                synchronized (this) {
                    if (!allowOverwrite() && !isWarningPrinted) {
                        U.warn(log, "Data streamer will not overwrite existing cache entries for better performance " +
                            "(to change, set allowOverwrite to true)");
                    }

                    isWarningPrinted = true;
                }
            }

            Map<ClusterNode, Collection<DataStreamerEntry>> mappings = new HashMap<>();

            boolean initPda = ctx.deploy().enabled() && jobPda == null;

            GridCacheAdapter cache = ctx.cache().internalCache(cacheName);

            if (cache == null)
                throw new IgniteCheckedException("Cache not created or already destroyed.");

            GridCacheContext cctx = cache.context();

            GridCacheGateway gate = null;

            AffinityTopologyVersion topVer;

            if (!cctx.isLocal())
                topVer = ctx.cache().context().exchange().lastTopologyFuture().get();
            else
                topVer = ctx.cache().context().exchange().readyAffinityVersion();

            List<List<ClusterNode>> assignments = cctx.affinity().assignments(topVer);

            if (!allowOverwrite() && !cctx.isLocal()) { // Cases where cctx required.
                gate = cctx.gate();

                gate.enter();
            }

            try {
                for (DataStreamerEntry entry : entries) {
                    List<ClusterNode> nodes;

                    try {
                        KeyCacheObject key = entry.getKey();

                        assert key != null;

                        if (initPda) {
                            if (cacheObjCtx.addDeploymentInfo())
                                jobPda = new DataStreamerPda(key.value(cacheObjCtx, false),
                                    entry.getValue() != null ? entry.getValue().value(cacheObjCtx, false) : null,
                                    rcvr);
                            else if (rcvr != null)
                                jobPda = new DataStreamerPda(rcvr);

                            initPda = false;
                        }

                        if (key.partition() == -1)
                            key.partition(cctx.affinity().partition(key, false));

                        nodes = nodes(key, topVer, cctx);
                    }
                    catch (IgniteCheckedException e) {
                        resFut.onDone(e);

                        return;
                    }

                    if (F.isEmpty(nodes)) {
                        resFut.onDone(new ClusterTopologyException("Failed to map key to node " +
                            "(no nodes with cache found in topology) [infos=" + entries.size() +
                            ", cacheName=" + cacheName + ']'));

                        return;
                    }

                    for (ClusterNode node : nodes) {
                        Collection<DataStreamerEntry> col = mappings.get(node);

                        if (col == null)
                            mappings.put(node, col = new ArrayList<>());

                        col.add(entry);
                    }
                }

                for (final Map.Entry<ClusterNode, Collection<DataStreamerEntry>> e : mappings.entrySet()) {
                    final UUID nodeId = e.getKey().id();

                    Buffer buf = bufMappings.get(nodeId);

                    if (buf == null) {
                        Buffer old = bufMappings.putIfAbsent(nodeId, buf = new Buffer(e.getKey()));

                        if (old != null)
                            buf = old;
                    }

                    final Collection<DataStreamerEntry> entriesForNode = e.getValue();

                    IgniteInClosure<IgniteInternalFuture<?>> lsnr = new IgniteInClosure<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> t) {
                            try {
                                t.get();

                                if (activeKeys != null) {
                                    for (DataStreamerEntry e : entriesForNode)
                                        activeKeys.remove(new KeyCacheObjectWrapper(e.getKey()));

                                    if (activeKeys.isEmpty())
                                        resFut.onDone();
                                }
                                else {
                                    assert entriesForNode.size() == 1;

                                    // That has been a single key,
                                    // so complete result future right away.
                                    resFut.onDone();
                                }
                            }
                            catch (IgniteClientDisconnectedCheckedException e1) {
                                if (log.isDebugEnabled())
                                    log.debug("Future finished with disconnect error [nodeId=" + nodeId + ", err=" + e1 + ']');

                                resFut.onDone(e1);
                            }
                            catch (IgniteCheckedException e1) {
                                if (log.isDebugEnabled())
                                    log.debug("Future finished with error [nodeId=" + nodeId + ", err=" + e1 + ']');

                                if (cancelled) {
                                    resFut.onDone(new IgniteCheckedException("Data streamer has been cancelled: " +
                                        DataStreamerImpl.this, e1));
                                }
                                else if (remaps + 1 > maxRemapCnt) {
                                    resFut.onDone(new IgniteCheckedException("Failed to finish operation (too many remaps): "
                                        + remaps, e1));
                                }
                                else {
                                    try {
                                        remapSem.acquire();

                                        final Runnable r = new Runnable() {
                                            @Override public void run() {
                                                try {
                                                    if (cancelled)
                                                        closedException();

                                                    load0(entriesForNode, resFut, activeKeys, remaps + 1);
                                                }
                                                catch (Throwable ex) {
                                                    resFut.onDone(
                                                        new IgniteCheckedException("DataStreamer remapping failed. ", ex));
                                                }
                                                finally {
                                                    remapSem.release();
                                                }
                                            }
                                        };

                                        dataToRemap.add(r);

                                        if (!remapOwning.get() && remapOwning.compareAndSet(false, true)) {
                                            ctx.closure().callLocalSafe(new GPC<Boolean>() {
                                                @Override public Boolean call() {
                                                    boolean locked = true;

                                                    while (locked || !dataToRemap.isEmpty()) {
                                                        if (!locked && !remapOwning.compareAndSet(false, true))
                                                            return false;

                                                        try {
                                                            Runnable r = dataToRemap.poll();

                                                            if (r != null)
                                                                r.run();
                                                        }
                                                        finally {
                                                            if (!dataToRemap.isEmpty())
                                                                locked = true;
                                                            else {
                                                                remapOwning.set(false);

                                                                locked = false;
                                                            }
                                                        }
                                                    }

                                                    return true;
                                                }
                                            }, true);
                                        }
                                    }
                                    catch (InterruptedException e2) {
                                        resFut.onDone(e2);
                                    }
                                }
                            }
                        }
                    };

                    GridCompoundFuture opFut = new SilentCompoundFuture();

                    opFut.listen(lsnr);

                    final List<GridFutureAdapter<?>> futs;

                    try {
                        futs = buf.update(entriesForNode, topVer, assignments, opFut, remap);

                        opFut.markInitialized();
                    }
                    catch (IgniteInterruptedCheckedException e1) {
                        resFut.onDone(e1);

                        return;
                    }

                    if (ctx.discovery().node(nodeId) == null) {
                        if (bufMappings.remove(nodeId, buf)) {
                            final Buffer buf0 = buf;

                            waitAffinityAndRun(new Runnable() {
                                @Override public void run() {
                                    buf0.onNodeLeft();

                                    if (futs != null) {
                                        Throwable ex = new ClusterTopologyCheckedException(
                                                "Failed to wait for request completion (node has left): " + nodeId);

                                        for (int i = 0; i < futs.size(); i++)
                                            futs.get(i).onDone(ex);
                                    }
                                }
                            }, ctx.discovery().topologyVersion(), false);
                        }
                    }
                }
            }
            finally {
                if (gate != null)
                    gate.leave();
            }
        }
        catch (Exception ex) {
            resFut.onDone(new IgniteCheckedException("DataStreamer data loading failed.", ex));
        }
    }

    /**
     * Throws stream closed exception.
     */
    private void closedException() {
        throw new IllegalStateException("Data streamer has been closed.", cancellationReason);
    }

    /**
     * @param key Key to map.
     * @param topVer Topology version.
     * @param cctx Context.
     * @return Nodes to send requests to.
     * @throws IgniteCheckedException If failed.
     */
    private List<ClusterNode> nodes(KeyCacheObject key,
        AffinityTopologyVersion topVer,
        GridCacheContext cctx) throws IgniteCheckedException {
        GridAffinityProcessor aff = ctx.affinity();

        List<ClusterNode> res = null;

        if (!allowOverwrite())
            res = cctx.isLocal() ?
                aff.mapKeyToPrimaryAndBackups(cacheName, key, topVer) :
                cctx.topology().nodes(cctx.affinity().partition(key), topVer);
        else {
            ClusterNode node = aff.mapKeyToNode(cacheName, key, topVer);

            if (node != null)
                res = Collections.singletonList(node);
        }

        if (F.isEmpty(res))
            throw new ClusterTopologyServerNotFoundException("Failed to find server node for cache (all affinity " +
                "nodes have left the grid or cache was stopped): " + cacheName);

        return res;
    }

    /**
     * Performs flush.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void doFlush() throws IgniteCheckedException {
        lastFlushTime = U.currentTimeMillis();

        List<IgniteInternalFuture> activeFuts0 = null;

        int doneCnt = 0;

        for (IgniteInternalFuture<?> f : activeFuts) {
            if (!f.isDone()) {
                if (activeFuts0 == null)
                    activeFuts0 = new ArrayList<>((int)(activeFuts.size() * 1.2));

                activeFuts0.add(f);
            }
            else {
                f.get();

                doneCnt++;
            }
        }

        if (activeFuts0 == null || activeFuts0.isEmpty())
            return;

        while (true) {
            Queue<IgniteInternalFuture<?>> q = null;

            for (Buffer buf : bufMappings.values()) {
                IgniteInternalFuture<?> flushFut = buf.flush();

                if (flushFut != null) {
                    if (q == null)
                        q = new ArrayDeque<>(bufMappings.size() * 2);

                    q.add(flushFut);
                }
            }

            if (q != null) {
                assert !q.isEmpty();

                boolean err = false;

                long startTimeMillis = U.currentTimeMillis();

                for (IgniteInternalFuture fut = q.poll(); fut != null; fut = q.poll()) {
                    try {
                        if (timeout == DFLT_UNLIMIT_TIMEOUT)
                            fut.get();
                        else {
                            long timeRemain = timeout - U.currentTimeMillis() + startTimeMillis;

                            if (timeRemain <= 0)
                                throw new IgniteDataStreamerTimeoutException("Data streamer exceeded timeout on flush.");

                            fut.get(timeRemain);
                        }
                    }
                    catch (IgniteClientDisconnectedCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to flush buffer: " + e);

                        throw CU.convertToCacheException(e);
                    }
                    catch (IgniteFutureTimeoutCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to flush buffer: " + e);

                        throw new IgniteDataStreamerTimeoutException("Data streamer exceeded timeout on flush.", e);
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to flush buffer: " + e);

                        err = true;
                    }
                }

                if (err)
                    // Remaps needed - flush buffers.
                    continue;
            }

            doneCnt = 0;

            for (int i = 0; i < activeFuts0.size(); i++) {
                IgniteInternalFuture f = activeFuts0.get(i);

                if (f == null)
                    doneCnt++;
                else if (f.isDone()) {
                    f.get();

                    doneCnt++;

                    activeFuts0.set(i, null);
                }
                else
                    break;
            }

            if (doneCnt == activeFuts0.size())
                return;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public void flush() throws CacheException {
        enterBusy();

        try {
            streamerLock.writeLock().lock();

            if (threadBufMap.get(Thread.currentThread().getId()).get2().size() > 0) {
                loadData(threadBufMap.get(Thread.currentThread().getId()).get2(),
                        threadBufMap.get(Thread.currentThread().getId()).get1());

                clearBuf();
            }

            streamerLock.writeLock().unlock();

            doFlush();
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Flushes every internal buffer if buffer was flushed before passed in
     * threshold.
     * <p>
     * Does not wait for result and does not fail on errors assuming that this method
     * should be called periodically.
     */
    @Override public void tryFlush() throws IgniteInterruptedException {
        if (!busyLock.enterBusy())
            return;

        try {
            streamerLock.writeLock().lock();

            for (Long threadId :
                    threadBufMap.keySet()) {
                if (threadBufMap.get(threadId).get2().size() > 0)
                    loadData(threadBufMap.get(threadId).get2(), threadBufMap.get(threadId).get1());
            }

            streamerLock.writeLock().unlock();

            for (Buffer buf : bufMappings.values())
                buf.flush();

            lastFlushTime = U.currentTimeMillis();
        }
        catch (IgniteInterruptedCheckedException e) {
            throw GridCacheUtils.convertToCacheException(e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param cancel {@code True} to close with cancellation.
     * @throws CacheException If failed.
     */
    @Override public void close(boolean cancel) throws CacheException {
        try {
            streamerLock.writeLock().lock();

            closedStreamer = true;

                for (Long threadId :
                        threadBufMap.keySet()) {
                    if (threadBufMap.get(threadId).get2().size() > 0)
                        loadData(threadBufMap.get(threadId).get2(), threadBufMap.get(threadId).get1());
                }
            streamerLock.writeLock().unlock();

            closeEx(cancel);
        }
        catch (IgniteCheckedException e) {
            throw CU.convertToCacheException(e);
        }
    }

    /**
     * @param cancel {@code True} to close with cancellation.
     * @throws IgniteCheckedException If failed.
     */
    public void closeEx(boolean cancel) throws IgniteCheckedException {
        IgniteCheckedException err = closeEx(cancel, null);

        if (err != null)
            throw err; // Throws at close().
    }

    /**
     * @param cancel {@code True} to close with cancellation.
     * @param err Error.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteCheckedException closeEx(boolean cancel, IgniteCheckedException err) throws IgniteCheckedException {
        if (!closed.compareAndSet(false, true))
            return null;

        busyLock.block();

        if (log.isDebugEnabled())
            log.debug("Closing data streamer [ldr=" + this + ", cancel=" + cancel + ']');

        try {
            // Assuming that no methods are called on this loader after this method is called.
            if (cancel) {
                cancelled = true;

                for (Buffer buf : bufMappings.values())
                    buf.cancelAll(err);
            }
            else
                doFlush();

            ctx.event().removeLocalEventListener(discoLsnr);

            ctx.io().removeMessageListener(topic);
        }
        catch (IgniteCheckedException | IgniteDataStreamerTimeoutException e) {
            fut.onDone(e);
            throw e;
        }

        long failed = failCntr.longValue();

        if (failed > 0 && err == null)
            err = new IgniteCheckedException("Some of DataStreamer operations failed [failedCount=" + failed + "]");

        fut.onDone(err);

        return err;
    }

    /**
     * @param reconnectFut Reconnect future.
     * @throws IgniteCheckedException If failed.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        IgniteClientDisconnectedCheckedException err = new IgniteClientDisconnectedCheckedException(reconnectFut,
            "Data streamer has been closed, client node disconnected.");

        disconnectErr = (CacheException)CU.convertToCacheException(err);

        for (Buffer buf : bufMappings.values())
            buf.cancelAll(err);

        closeEx(true, err);
    }

    /**
     * @return {@code true} If the loader is closed.
     */
    boolean isClosed() {
        return fut.isDone();
    }

    /** {@inheritDoc} */
    @Override public void close() throws CacheException {
        close(false);
    }

    /**
     * @return Max remap count.
     */
    public int maxRemapCount() {
        return maxRemapCnt;
    }

    /**
     * @param maxRemapCnt New max remap count.
     */
    public void maxRemapCount(int maxRemapCnt) {
        this.maxRemapCnt = maxRemapCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override public long getDelay(TimeUnit unit) {
        return unit.convert(nextFlushTime() - U.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @return Next flush time.
     */
    private long nextFlushTime() {
        return lastFlushTime + autoFlushFreq;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(Delayed o) {
        return nextFlushTime() > ((DataStreamerImpl)o).nextFlushTime() ? 1 : -1;
    }

    /**
     * Check permissions for streaming.
     *
     * @param perm Security permission.
     * @throws org.apache.ignite.plugin.security.SecurityException If permissions are not enough for streaming.
     */
    private void checkSecurityPermission(SecurityPermission perm)
        throws org.apache.ignite.plugin.security.SecurityException {
        if (!ctx.security().enabled())
            return;

        ctx.security().authorize(cacheName, perm, null);
    }

    /**
     * Ensures that cache has been started and is ready to store streamed data.
     */
    private void ensureCacheStarted() {
        DynamicCacheDescriptor desc = ctx.cache().cacheDescriptor(cacheName);

        assert desc != null;

        if (desc.startTopologyVersion() == null)
            return;

        IgniteInternalFuture<?> affReadyFut = ctx.cache().context().exchange()
            .affinityReadyFuture(desc.startTopologyVersion());

        if (affReadyFut != null) {
            try {
                affReadyFut.get();
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
        }
    }

    /**
     *
     */
    private class Buffer {
        /** Node. */
        private final ClusterNode node;

        /** Active futures. */
        private final Collection<IgniteInternalFuture<Object>> locFuts;

        /** Buffered entries. */
        private final PerStripeBuffer[] stripes;

        /** Local node flag. */
        private final boolean isLocNode;

        /** ID generator. */
        private final AtomicLong idGen = new AtomicLong();

        /** Active futures. */
        private final ConcurrentMap<Long, GridFutureAdapter<Object>> reqs;

        /** */
        private final Semaphore sem;

        /** */
        private final int perNodeParallelOps;

        /** Closure to signal on task finish. */
        @GridToStringExclude
        private final IgniteInClosure<IgniteInternalFuture<Object>> signalC =
            new IgniteInClosure<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> t) {
                    signalTaskFinished(t);
                }
            };

        /**
         * @param node Node.
         */
        Buffer(ClusterNode node) {
            assert node != null;

            this.node = node;

            locFuts = new GridConcurrentHashSet<>();
            reqs = new ConcurrentHashMap8<>();

            // Cache local node flag.
            isLocNode = node.equals(ctx.discovery().localNode());

            Integer attrStreamerPoolSize = node.attribute(IgniteNodeAttributes.ATTR_DATA_STREAMER_POOL_SIZE);

            int streamerPoolSize = attrStreamerPoolSize != null ? attrStreamerPoolSize : node.metrics().getTotalCpus();

            perNodeParallelOps = parallelOps != 0 ? parallelOps :
                streamerPoolSize * IgniteDataStreamer.DFLT_PARALLEL_OPS_MULTIPLIER;

            sem = new Semaphore(perNodeParallelOps);

            stripes = (PerStripeBuffer[])Array.newInstance(PerStripeBuffer.class, streamerPoolSize);

            for (int i = 0; i < stripes.length; i++)
                stripes[i] = new PerStripeBuffer(i, signalC);
        }

        /**
         * @param newEntries Infos.
         * @param topVer Topology version.
         * @param opFut Completion future for the operation.
         * @param remap Remapping flag.
         * @return Future for operation.
         * @throws IgniteInterruptedCheckedException If failed.
         */
        @Nullable List<GridFutureAdapter<?>> update(
            Iterable<DataStreamerEntry> newEntries,
            AffinityTopologyVersion topVer,
            List<List<ClusterNode>> assignments,
            GridCompoundFuture opFut,
            boolean remap
        ) throws IgniteInterruptedCheckedException {
            List<GridFutureAdapter<?>> res = null;
            GridFutureAdapter[] futs = new GridFutureAdapter[stripes.length];

            for (DataStreamerEntry entry : newEntries) {
                List<DataStreamerEntry> entries0 = null;
                AffinityTopologyVersion curBatchTopVer;

                // Init buffer.
                int part = entry.getKey().partition();

                GridFutureAdapter<Object> curFut0;
                PerStripeBuffer b = stripes[part % stripes.length];

                synchronized (b) {
                    curFut0 = b.curFut;

                    if (futs[b.partId] != curFut0) {
                        opFut.add(curFut0);

                        if (res == null)
                            res = new ArrayList<>();

                        res.add(curFut0);

                        futs[b.partId] = curFut0;
                    }

                    if (b.batchTopVer == null) {
                        b.batchTopVer = topVer;

                        b.assignments = assignments;
                    }

                    // topology changed, but affinity is the same, no re-map is required.
                    if (!topVer.equals(b.batchTopVer) && b.assignments.equals(assignments)) {
                        b.batchTopVer = topVer;

                        b.assignments = assignments;
                    }

                    curBatchTopVer = b.batchTopVer;

                    b.entries.add(entry);

                    if (b.entries.size() >= bufSize) {
                        entries0 = b.entries;

                        b.renewBatch(remap);
                    }
                }

                if (!allowOverwrite() && !topVer.equals(curBatchTopVer)) {
                    for (int i = 0; i < stripes.length; i++) {
                        PerStripeBuffer b0 = stripes[i];

                        // renew all stale versions
                        synchronized (b0) {
                            // Another thread might already renew the batch
                            AffinityTopologyVersion bTopVer = b0.batchTopVer;

                            if(bTopVer != null && topVer.compareTo(bTopVer) > 0) {
                                GridFutureAdapter<Object> bFut = b0.curFut;

                                b0.renewBatch(remap);

                                bFut.onDone(null,
                                    new IgniteCheckedException("Topology changed during batch preparation " +
                                        "[batchTopVer=" + bTopVer + ", topVer=" + topVer + "]"));
                            }
                        }
                    }

                    // double check, it's possible that current future was already overwritten on buffer overflow
                    curFut0.onDone(null, new IgniteCheckedException("Topology changed during batch preparation." +
                        "[batchTopVer=" + curBatchTopVer + ", topVer=" + topVer + "]"));
                }
                else if (entries0 != null) {
                    submit(entries0, curBatchTopVer, curFut0, remap, b.partId);

                    if (cancelled)
                        curFut0.onDone(new IgniteCheckedException("Data streamer has been cancelled: " +
                            DataStreamerImpl.this));
                    else if (ctx.clientDisconnected())
                        curFut0.onDone(new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                            "Client node disconnected."));
                }
            }

            return res;
        }

        /**
         * @return Future if any submitted.
         * @throws IgniteInterruptedCheckedException If thread has been interrupted.
         */
        @Nullable IgniteInternalFuture<?> flush() throws IgniteInterruptedCheckedException {
            acquireRemapSemaphore();

            for (PerStripeBuffer b : stripes) {
                AffinityTopologyVersion batchTopVer = null;
                List<DataStreamerEntry> entries0 = null;
                GridFutureAdapter<Object> curFut0 = null;

                synchronized (b) {
                    if (!b.entries.isEmpty()) {
                        entries0 = b.entries;
                        curFut0 = b.curFut;
                        batchTopVer = b.batchTopVer;

                        b.renewBatch(false);
                    }
                }

                if (entries0 != null)
                    submit(entries0, batchTopVer, curFut0, false, b.partId);
            }

            // Create compound future for this flush.
            GridCompoundFuture<Object, Object> res = null;

            for (IgniteInternalFuture<Object> f : locFuts) {
                if (res == null)
                    res = new GridCompoundFuture<>();

                res.add(f);
            }

            for (IgniteInternalFuture<Object> f : reqs.values()) {
                if (res == null)
                    res = new GridCompoundFuture<>();

                res.add(f);
            }

            if (res != null)
                res.markInitialized();

            return res;
        }

        /**
         * Increments active tasks count.
         *
         * @throws IgniteInterruptedCheckedException If thread has been interrupted.
         */
        private void incrementActiveTasks() throws IgniteInterruptedCheckedException {
            if (timeout == DFLT_UNLIMIT_TIMEOUT)
                U.acquire(sem);
            else if (!U.tryAcquire(sem, timeout, TimeUnit.MILLISECONDS)) {
                if (log.isDebugEnabled())
                    log.debug("Failed to add parallel operation.");

                throw new IgniteDataStreamerTimeoutException("Data streamer exceeded timeout when starts parallel operation.");
            }
        }

        /**
         * @param f Future that finished.
         */
        private void signalTaskFinished(IgniteInternalFuture<Object> f) {
            assert f != null;

            sem.release();
        }

        /**
         * @param entries Entries.
         * @param reqTopVer Request topology version.
         * @param curFut Current future.
         * @param plc Policy.
         */
        private void localUpdate(final Collection<DataStreamerEntry> entries,
            final AffinityTopologyVersion reqTopVer,
            final GridFutureAdapter<Object> curFut,
            final byte plc) {
            try {
                GridCacheContext cctx = ctx.cache().internalCache(cacheName).context();

                final boolean lockTop = !cctx.isLocal() && !allowOverwrite();

                GridDhtTopologyFuture topWaitFut = null;

                if (lockTop)
                    cctx.topology().readLock();

                try {
                    AffinityTopologyVersion streamerFutTopVer = null;

                    if (lockTop) {
                        GridDhtTopologyFuture topFut = cctx.topologyVersionFuture();

                        AffinityTopologyVersion topVer = topFut.isDone() ? topFut.topologyVersion() :
                            topFut.initialVersion();

                        if (topVer.compareTo(reqTopVer) > 0) {
                            curFut.onDone(new IgniteCheckedException("DataStreamer will retry data transfer " +
                                "at stable topology. reqTop=" + reqTopVer + ", topVer=" + topFut.initialVersion() +
                                ", node=local]"));

                            return;
                        }
                        else if (!topFut.isDone())
                            topWaitFut = topFut;
                        else
                            streamerFutTopVer = topFut.topologyVersion();
                    }

                    if (topWaitFut == null) {
                        IgniteInternalFuture<Object> callFut = ctx.closure().callLocalSafe(
                            new DataStreamerUpdateJob(
                                ctx,
                                log,
                                cacheName,
                                entries,
                                false,
                                skipStore,
                                keepBinary,
                                rcvr),
                            plc);

                        locFuts.add(callFut);

                        final GridFutureAdapter waitFut =
                            lockTop ? cctx.mvcc().addDataStreamerFuture(streamerFutTopVer) : null;

                        callFut.listen(new IgniteInClosure<IgniteInternalFuture<Object>>() {
                            @Override public void apply(IgniteInternalFuture<Object> t) {
                                try {
                                    boolean rmv = locFuts.remove(t);

                                    assert rmv;

                                    curFut.onDone(t.get());
                                }
                                catch (IgniteCheckedException e) {
                                    curFut.onDone(e);
                                }
                                finally {
                                    if (waitFut != null)
                                        waitFut.onDone();
                                }
                            }
                        });
                    }
                }
                finally {
                    if (lockTop)
                        cctx.topology().readUnlock();
                }

                if (topWaitFut != null) {
                    // Need call 'listen' after topology read lock is released.
                    topWaitFut.listen(new IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> e) {
                            localUpdate(entries, reqTopVer, curFut, plc);
                        }
                    });
                }
            }
            catch (Throwable ex) {
                curFut.onDone(new IgniteCheckedException("DataStreamer data handling failed.", ex));
            }
        }

        /**
         * @param entries Entries to submit.
         * @param topVer Topology version.
         * @param curFut Current future.
         * @param remap Remapping flag.
         * @param partId Partition ID.
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private void submit(
            final Collection<DataStreamerEntry> entries,
            @Nullable AffinityTopologyVersion topVer,
            final GridFutureAdapter<Object> curFut,
            boolean remap,
            int partId
        ) throws IgniteInterruptedCheckedException {
            assert entries != null;
            assert !entries.isEmpty();
            assert curFut != null;

            if (!remap) {
                try {
                    incrementActiveTasks();
                }
                catch (IgniteDataStreamerTimeoutException e) {
                    curFut.onDone(e);

                    throw e;
                }
            }

            IgniteInternalFuture<Object> fut;

            byte plc = DataStreamProcessor.ioPolicy(ioPlcRslvr, node);

            if (isLocNode)
                localUpdate(entries, topVer, curFut, plc);
            else {
                try {
                    for (DataStreamerEntry e : entries) {
                        e.getKey().prepareMarshal(cacheObjCtx);

                        CacheObject val = e.getValue();

                        if (val != null)
                            val.prepareMarshal(cacheObjCtx);
                    }

                    if (updaterBytes == null) {
                        assert rcvr != null;

                        updaterBytes = U.marshal(ctx, rcvr);
                    }

                    if (topicBytes == null)
                        topicBytes = U.marshal(ctx, topic);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to marshal (request will not be sent).", e);

                    return;
                }

                GridDeployment dep = null;
                GridPeerDeployAware jobPda0 = jobPda;

                if (ctx.deploy().enabled() && jobPda0 != null) {
                    try {
                        dep = ctx.deploy().deploy(jobPda0.deployClass(), jobPda0.classLoader());

                        GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

                        if (cache != null)
                            cache.context().deploy().onEnter();
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to deploy class (request will not be sent): " +
                            jobPda0.deployClass(), e);

                        return;
                    }

                    if (dep == null)
                        U.warn(log, "Failed to deploy class (request will be sent): " + jobPda0.deployClass());
                }

                long reqId = idGen.incrementAndGet();

                fut = curFut;

                reqs.put(reqId, (GridFutureAdapter<Object>)fut);

                if (topVer == null)
                    topVer = ctx.cache().context().exchange().readyAffinityVersion();

                DataStreamerRequest req = new DataStreamerRequest(
                    reqId,
                    topicBytes,
                    cacheName,
                    updaterBytes,
                    entries,
                    true,
                    skipStore,
                    keepBinary,
                    dep != null ? dep.deployMode() : null,
                    dep != null ? jobPda0.deployClass().getName() : null,
                    dep != null ? dep.userVersion() : null,
                    dep != null ? dep.participants() : null,
                    dep != null ? dep.classLoaderId() : null,
                    dep == null,
                    topVer,
                    (rcvr == ISOLATED_UPDATER) ?
                        partId : GridIoMessage.STRIPE_DISABLED_PART);

                try {
                    ctx.io().sendToGridTopic(node, TOPIC_DATASTREAM, req, plc);

                    if (log.isDebugEnabled())
                        log.debug("Sent request to node [nodeId=" + node.id() + ", req=" + req + ']');
                }
                catch (ClusterTopologyCheckedException e) {
                    GridFutureAdapter<Object> fut0 = ((GridFutureAdapter<Object>)fut);

                    fut0.onDone(e);
                }
                catch (IgniteCheckedException e) {
                    GridFutureAdapter<Object> fut0 = ((GridFutureAdapter<Object>)fut);

                    try {
                        if (ctx.discovery().alive(node) && ctx.discovery().pingNode(node.id()))
                            fut0.onDone(e);
                        else
                            fut0.onDone(new ClusterTopologyCheckedException("Failed to send request (node has left): "
                                + node.id()));
                    }
                    catch (IgniteClientDisconnectedCheckedException e0) {
                        fut0.onDone(e0);
                    }
                }
            }
        }

        /**
         *
         */
        void onNodeLeft() {
            assert !isLocNode;
            assert bufMappings.get(node.id()) != this;

            if (log.isDebugEnabled())
                log.debug("Forcibly completing futures (node has left): " + node.id());

            Exception e = new ClusterTopologyCheckedException("Failed to wait for request completion " +
                "(node has left): " + node.id());

            for (GridFutureAdapter<Object> f : reqs.values())
                f.onDone(e);

            // Make sure to complete current future.
            GridFutureAdapter<Object> curFut0;

            for (PerStripeBuffer b : stripes) {
                synchronized (b) {
                    curFut0 = b.curFut;
                }

                curFut0.onDone(e);
            }
        }

        /**
         * @param res Response.
         * @param nodeId Node id.
         */
        void onResponse(DataStreamerResponse res, UUID nodeId) {
            if (log.isDebugEnabled())
                log.debug("Received data load response: " + res);

            GridFutureAdapter<?> f = reqs.remove(res.requestId());

            if (f == null) {
                if (log.isDebugEnabled())
                    log.debug("Future for request has not been found: " + res.requestId());

                return;
            }

            Throwable err = null;

            byte[] errBytes = res.errorBytes();

            if (errBytes != null) {
                try {
                    GridPeerDeployAware jobPda0 = jobPda;

                    final Throwable cause = U.unmarshal(
                        ctx,
                        errBytes,
                        U.resolveClassLoader(jobPda0 != null ? jobPda0.classLoader() : null, ctx.config()));

                    final String msg = "DataStreamer request failed [node=" + nodeId + "]";

                    err = cause instanceof ClusterTopologyCheckedException ?
                        new ClusterTopologyCheckedException(msg, cause) :
                        new IgniteCheckedException(msg, cause);
                }
                catch (IgniteCheckedException e) {
                    f.onDone(null, new IgniteCheckedException("Failed to unmarshal response.", e));

                    return;
                }
            }

            f.onDone(null, err);

            if (log.isDebugEnabled())
                log.debug("Finished future [fut=" + f + ", reqId=" + res.requestId() + ", err=" + err + ']');
        }

        /**
         * @param err Error.
         */
        void cancelAll(@Nullable IgniteCheckedException err) {
            if (err == null)
                err = new IgniteCheckedException("Data streamer has been cancelled: " + DataStreamerImpl.this);

            for (IgniteInternalFuture<?> f : locFuts) {
                try {
                    f.cancel();
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to cancel mini-future.", e);
                }
            }

            for (GridFutureAdapter<?> f : reqs.values())
                f.onDone(err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            int size = 0;

            for (int i = 0; i < stripes.length; i++) {
                PerStripeBuffer b = stripes[i];

                synchronized (b) {
                    size += b.entries.size();
                }
            }

            return S.toString(Buffer.class, this,
                "entriesCnt", size,
                "locFutsSize", locFuts.size(),
                "reqsSize", reqs.size());
        }
    }

    /**
     * Data streamer peer-deploy aware.
     */
    private class DataStreamerPda implements GridPeerDeployAware {
        /** */
        private static final long serialVersionUID = 0L;

        /** Deploy class. */
        private Class<?> cls;

        /** Class loader. */
        private ClassLoader ldr;

        /** Collection of objects to detect deploy class and class loader. */
        private Collection<Object> objs;

        /**
         * Constructs data streamer peer-deploy aware.
         *
         * @param objs Collection of objects to detect deploy class and class loader.
         */
        private DataStreamerPda(Object... objs) {
            this.objs = Arrays.asList(objs);
        }

        /** {@inheritDoc} */
        @Override public Class<?> deployClass() {
            if (cls == null) {
                Class<?> cls0 = null;

                if (depCls != null)
                    cls0 = depCls;
                else {
                    for (Iterator<Object> it = objs.iterator(); (cls0 == null || U.isJdk(cls0)) && it.hasNext(); ) {
                        Object o = it.next();

                        if (o != null)
                            cls0 = U.detectClass(o);
                    }

                    if (cls0 == null || U.isJdk(cls0))
                        cls0 = DataStreamerImpl.class;
                }

                assert cls0 != null : "Failed to detect deploy class [objs=" + objs + ']';

                cls = cls0;
            }

            return cls;
        }

        /** {@inheritDoc} */
        @Override public ClassLoader classLoader() {
            if (ldr == null) {
                ClassLoader ldr0 = deployClass().getClassLoader();

                // Safety.
                if (ldr0 == null)
                    ldr0 = U.gridClassLoader();

                assert ldr0 != null : "Failed to detect classloader [objs=" + objs + ']';

                ldr = ldr0;
            }

            return ldr;
        }
    }

    /**
     * Isolated receiver which only loads entry initial value.
     */
    protected static class IsolatedUpdater implements StreamReceiver<KeyCacheObject, CacheObject>,
        DataStreamerCacheUpdaters.InternalUpdater {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void receive(
            IgniteCache<KeyCacheObject, CacheObject> cache,
            Collection<Map.Entry<KeyCacheObject, CacheObject>> entries
        ) {
            IgniteCacheProxy<KeyCacheObject, CacheObject> proxy = (IgniteCacheProxy<KeyCacheObject, CacheObject>)cache;

            GridCacheAdapter<KeyCacheObject, CacheObject> internalCache = proxy.context().cache();

            if (internalCache.isNear())
                internalCache = internalCache.context().near().dht();

            GridCacheContext cctx = internalCache.context();

            AffinityTopologyVersion topVer = cctx.isLocal() ?
                cctx.affinity().affinityTopologyVersion() :
                cctx.shared().exchange().readyAffinityVersion();

            GridCacheVersion ver = cctx.versions().isolatedStreamerVersion();

            long ttl = CU.TTL_ETERNAL;
            long expiryTime = CU.EXPIRE_TIME_ETERNAL;

            ExpiryPolicy plc = cctx.expiry();

            Collection<Integer> reservedParts = new HashSet<>();
            Collection<Integer> ignoredParts = new HashSet<>();

            try {
                for (Entry<KeyCacheObject, CacheObject> e : entries) {
                    cctx.shared().database().checkpointReadLock();

                    try {
                        e.getKey().finishUnmarshal(cctx.cacheObjectContext(), cctx.deploy().globalLoader());

                        if (!cctx.isLocal()) {
                            int p = cctx.affinity().partition(e.getKey());

                            if (ignoredParts.contains(p))
                                continue;

                            if (!reservedParts.contains(p)) {
                                GridDhtLocalPartition part = cctx.topology().localPartition(p, topVer, true);

                                if (!part.reserve()) {
                                    ignoredParts.add(p);

                                    continue;
                                }
                                else {
                                    // We must not allow to read from RENTING partitions.
                                    if (part.state() == GridDhtPartitionState.RENTING) {
                                        part.release();

                                        ignoredParts.add(p);

                                        continue;
                                    }

                                    reservedParts.add(p);
                                }
                            }
                        }

                        GridCacheEntryEx entry = internalCache.entryEx(e.getKey(), topVer);

                        if (plc != null) {
                            ttl = CU.toTtl(plc.getExpiryForCreation());

                            if (ttl == CU.TTL_ZERO)
                                continue;
                            else if (ttl == CU.TTL_NOT_CHANGED)
                                ttl = 0;

                            expiryTime = CU.toExpireTime(ttl);
                        }

                    boolean primary = cctx.affinity().primaryByKey(cctx.localNode(), entry.key(), topVer);

                    entry.initialValue(e.getValue(),
                        ver,
                        ttl,
                        expiryTime,
                        false,
                        topVer,
                        primary ? GridDrType.DR_LOAD : GridDrType.DR_PRELOAD,
                        false);

                        cctx.evicts().touch(entry, topVer);

                        CU.unwindEvicts(cctx);

                        entry.onUnlock();
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        ignoredParts.add(cctx.affinity().partition(e.getKey()));
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // No-op.
                    }
                    catch (IgniteCheckedException ex) {
                        IgniteLogger log = cache.unwrap(Ignite.class).log();

                        U.error(log, "Failed to set initial value for cache entry: " + e, ex);
                    }
                    finally {
                        cctx.shared().database().checkpointReadUnlock();
                    }
                }
            }
            finally {
                for (Integer part : reservedParts) {
                    GridDhtLocalPartition locPart = cctx.topology().localPartition(part, topVer, false);

                    assert locPart != null : "Evicted reserved partition: " + locPart;

                    locPart.release();
                }

                try {
                    if (!cctx.isNear() && cctx.shared().wal() != null)
                        cctx.shared().wal().fsync(null);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write preloaded entries into write-ahead log: " + e, e);
                }
            }
        }
    }

    /**
     * Key object wrapper. Using identity equals prevents slow down in case of hash code collision.
     */
    private static class KeyCacheObjectWrapper {
        /** key object */
        private final KeyCacheObject key;

        /**
         * Constructor
         *
         * @param key key object
         */
        KeyCacheObjectWrapper(KeyCacheObject key) {
            assert key != null;

            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return o instanceof KeyCacheObjectWrapper && this.key == ((KeyCacheObjectWrapper)o).key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(KeyCacheObjectWrapper.class, this);
        }
    }

    /**
     *
     */
    private class PerStripeBuffer {
        /** */
        private final int partId;

        /** */
        private List<DataStreamerEntry> entries;

        /** */
        private GridFutureAdapter<Object> curFut;

        /** Batch topology. */
        private AffinityTopologyVersion batchTopVer;

        /** */
        private final IgniteInClosure<? super IgniteInternalFuture<Object>> signalC;

        /** Batch assignments */
        public List<List<ClusterNode>> assignments;

        /**
         * @param partId Partition ID.
         * @param c Signal closure.
         */
        public PerStripeBuffer(
            int partId,
            IgniteInClosure<? super IgniteInternalFuture<Object>> c
        ) {
            this.partId = partId;
            signalC = c;

            renewBatch(false);
        }

        /**
         * @param remap Remap.
         */
        synchronized void renewBatch(boolean remap) {
            entries = newEntries();
            curFut = new GridFutureAdapter<>();

            batchTopVer = null;

            if (!remap)
                curFut.listen(signalC);
        }

        /**
         * @return Fresh collection with some space for outgrowth.
         */
        private List<DataStreamerEntry> newEntries() {
            return new ArrayList<>((int)(bufSize * 1.2));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PerStripeBuffer.class, this, super.toString());
        }
    }

    /** */
    private static final class SilentCompoundFuture<T,R> extends GridCompoundFuture<T,R> {
       /** {@inheritDoc} */
        @Override protected void logError(IgniteLogger log, String msg, Throwable e) {
            // no-op
        }

        /** {@inheritDoc} */
        @Override protected void logDebug(IgniteLogger log, String msg) {
            // no-op
        }
    }
}
