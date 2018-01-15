package org.apache.ignite.internal.processors.datastreamer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class DataStreamerAddDataTest extends GridCommonAbstractTest {
    static ExecutorService executor = Executors.newFixedThreadPool(1);

    /** Config. */
    private IgniteConfiguration cfg;

    /** String logger. */
    private GridStringLogger strLog = new GridStringLogger();

    /** Ignite data streamer. */
    private static DataStreamerImpl<Integer, Integer> dataLdr;

    /** Client. */
    private static Ignite client;

    /** Server 1. */
    private static Ignite srv1;

    /** Server 2. */
    private static Ignite srv2;
    private int DATA_AMOUNT = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.out.println("start1");

        srv1 = startGrid("server1");

        srv2 = startGrid("server2");

        client = startGrid("client");

        dataLdr = (DataStreamerImpl)client.dataStreamer(cfg.getCacheConfiguration()[0].getName());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.out.println("stop test");

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
        List<IgniteFuture> list = new ArrayList<>();
        dataLdr.setBatchSizePerKeyVal(5);
        for (int i = 1; i <= 20; i++) {
            final int indx = 0;
            System.out.println(i);
            list.add(dataLdr.addData(i, i));
            if (list.size() > 1 && i % 5 == 0) {

                list.get(list.size() - 1).listen(new IgniteInClosure<IgniteFuture<?>>() {
                    @Override
                    public void apply(IgniteFuture<?> igniteFuture) {
                        igniteFuture.get();
                        System.out.println("!!!~ done " + indx);
                    }

                });
            }

            if(list.size() > 1)
                System.out.println(list.get(list.size() - 1).equals(list.get(list.size() - 2)));

        }
//        for (IgniteFuture f :
//            list) {
//            System.out.println(f.isDone());
//        }
        dataLdr.close();

    }

    /**
     *
     */
    public void testAddDataCollection() throws Exception {
        Collection<java.util.AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {

            i++;
            final int indx = i;
//            System.out.println(i);
//            for (; i % ENTRY_AMOUNT != 0; i++)

            testList.add(new HashMap.SimpleEntry<>(i, i));

//            System.out.println("Collection: " + (timeAfter - timeBefore));
//            igniteFuture.listen(new IgniteInClosure<IgniteFuture<?>>() {
//                @Override
//                public void apply(IgniteFuture<?> igniteFuture) {
//                    igniteFuture.get();
//
//                    System.out.println("!!!~ done " + indx);
//                }
//            });
        }
        IgniteFuture<?> igniteFuture = dataLdr.addData(testList);

        igniteFuture.listen(new IgniteInClosure<IgniteFuture<?>>() {
            @Override
            public void apply(IgniteFuture<?> igniteFuture) {
//                        igniteFuture.get();
                System.out.println("!!!~ done ");
            }
        });

        System.out.println(igniteFuture.isCancelled());
//        dataLdr.flush();
//        System.out.println(igniteFuture.get());
    }
}