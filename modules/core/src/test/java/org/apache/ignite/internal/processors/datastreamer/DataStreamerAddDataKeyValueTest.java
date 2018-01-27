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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class DataStreamerAddDataKeyValueTest extends GridCommonAbstractTest {
    /** List of launching futures. */
    private List<IgniteFuture> futures = new ArrayList<>();

    /** Data amount. */
    private int DATA_AMOUNT = 1000;

    /** Buffer size. */
    private final int VALUES_PER_BATCH = 777;

    /** Config. */
    private IgniteConfiguration cfg;

    /** Ignite data streamer. */
    private static DataStreamerImpl<Integer, Integer> dataLdr;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("server1");

        startGrid("server2");

        Ignite client = startGrid("client");

        dataLdr = (DataStreamerImpl)client.dataStreamer(cfg.getCacheConfiguration()[0].getName());

        dataLdr.perBatchBufferSize(VALUES_PER_BATCH);

        dataLdr.perNodeBufferSize(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        futures.clear();

        dataLdr.flush();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));

        if (igniteInstanceName.equals("client"))
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Check that IgniteFuture will be returned per batch.
     */
    public void testSimilarFuturePerBatch() throws Exception {
        for (int i = 1; i <= DATA_AMOUNT; i++) {
            futures.add(dataLdr.addData(i, i));

            if (futures.size() > 1) {
                if (futures.size() % VALUES_PER_BATCH == 1)
                    assertFalse(futures.get(futures.size() - 1).equals(futures.get(futures.size() - 2)));
                else
                    assertTrue(futures.get(futures.size() - 1).equals(futures.get(futures.size() - 2)));
            }
        }
    }

    /**
     * Check that all IgniteFutures that should be streamed are done.
     */
    public void testAllFuturesAreDone() throws Exception {
        for (int i = 1; i <= DATA_AMOUNT; i++)
            futures.add(dataLdr.addData(i, i));

        dataLdr.flush();

        for (IgniteFuture future :
            futures)
            assertTrue(future.isDone());
    }

    /**
     * Check that amount of batches is appropriate.
     */
    public void testFuturesAmount() {
        HashSet uniqFut = new HashSet();

        double batchAmount = Math.ceil((double)DATA_AMOUNT / VALUES_PER_BATCH);

        for (int i = 1; i <= DATA_AMOUNT; i++)
            uniqFut.add(dataLdr.addData(i, i));

        assertTrue(uniqFut.size() == batchAmount);
    }

    /**
     *
     */
    public void testLoadingTimeout() throws Exception {
        Field timeField = dataLdr.getClass().getDeclaredField("lastLoadTime");

        timeField.setAccessible(true);

        long loadTimeBefore = Long.valueOf(timeField.get(dataLdr).toString());

        dataLdr.batchTimeout(10);

        dataLdr.addData(1, 1);

        TimeUnit.MILLISECONDS.sleep(100);

        long loadTimeAfter = Long.valueOf(timeField.get(dataLdr).toString());

        assertTrue(loadTimeBefore != loadTimeAfter);
    }

}