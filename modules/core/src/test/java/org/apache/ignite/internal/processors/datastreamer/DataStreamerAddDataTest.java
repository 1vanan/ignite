package org.apache.ignite.internal.processors.datastreamer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class DataStreamerAddDataTest extends GridCommonAbstractTest {
    static ExecutorService executor = Executors.newFixedThreadPool(1);

    /** Config. */
    private IgniteConfiguration cfg;

    /** Time before. */
    private long timeBefore;

    /** Time after. */
    private long timeAfter;

    /** Entry amount. */
    private static final Integer ENTRY_AMOUNT = 10;

    /** String logger. */
    private GridStringLogger strLog = new GridStringLogger();

    /** Ignite data streamer. */
    private static IgniteDataStreamer<Integer, Integer> dataLdr;

    /** Client. */
    private static Ignite client;

    /** Server 1. */
    private static Ignite srv1;

    /** Server 2. */
    private static Ignite srv2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv1 = startGrid("server1");

        srv2 = startGrid("server2");

        client = startGrid("client");

        dataLdr = client.dataStreamer(cfg.getCacheConfiguration()[0].getName());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        dataLdr.close();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));

        if (igniteInstanceName.equals("client"))
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(ccfg);

        cfg.setGridLogger(strLog);

        return cfg;
    }

    /**
     *
     */
    public void testAddDataKeyValue() throws Exception {
        int i = 0;

        timeBefore = U.currentTimeMillis();
        while (true) {
            final int indx = i;
            System.out.println(i);
            IgniteFuture igniteFuture = dataLdr.addData(i, i);
            igniteFuture.listen(new IgniteInClosure<IgniteFuture<?>>() {
                @Override
                public void apply(IgniteFuture<?> igniteFuture) {
//                        igniteFuture.get();

                    System.out.println("!!!~ done " + indx);
                }
            });
//            igniteFuture.get();
            i++;
        }

//        timeAfter = U.currentTimeMillis();

//        System.out.println("Key/Value: " + (timeAfter - timeBefore));
    }

    /**
     *
     */
    public void testAddDataCollection() throws Exception {
        int i = 0;

        while (true) {
            Collection<java.util.AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();

            i++;
            final int indx = i;
            System.out.println(i);
//            for (; i % ENTRY_AMOUNT != 0; i++)
            testList.add(new HashMap.SimpleEntry<>(i, i));

            timeBefore = U.currentTimeMillis();

            IgniteFuture<?> igniteFuture = dataLdr.addData(testList);

            timeAfter = U.currentTimeMillis();

//            System.out.println("Collection: " + (timeAfter - timeBefore));
            igniteFuture.listen(new IgniteInClosure<IgniteFuture<?>>() {
                @Override
                public void apply(IgniteFuture<?> igniteFuture) {
                    igniteFuture.get();

                    System.out.println("!!!~ done " + indx);
                }
            });
        }
//        dataLdr.flush();
//        System.out.println(igniteFuture.get());
    }
}