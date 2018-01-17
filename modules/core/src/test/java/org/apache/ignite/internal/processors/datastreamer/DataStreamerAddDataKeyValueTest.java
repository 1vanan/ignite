package org.apache.ignite.internal.processors.datastreamer;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class DataStreamerAddDataKeyValueTest extends GridCommonAbstractTest {
    /** List of launching futures. */
    List<IgniteFuture> futures = new ArrayList<>();

    /** Buffer size. */
    private final int VALUES_PER_BATCH = 5;

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

    private int DATA_AMOUNT = 1000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.out.println("start1");

        srv1 = startGrid("server1");

        srv2 = startGrid("server2");

        client = startGrid("client");

        dataLdr = (DataStreamerImpl)client.dataStreamer(cfg.getCacheConfiguration()[0].getName());

        dataLdr.setBufStreamerSizePerKeyVal(VALUES_PER_BATCH);

        dataLdr.setBufStreamerSizePerKeyVal(1000);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

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
     * Check than IgniteFuture will be returned per batch.
     */
    public void testSimilarFuturePerBatch() {
        for (int i = 1; i <= DATA_AMOUNT; i++)
            futures.add(dataLdr.addData(i, i));



        if (futures.size() > 1) {
            if (futures.size() % VALUES_PER_BATCH == 1)
                assertFalse(futures.get(futures.size() - 1).equals(futures.get(futures.size() - 2)));
            else
                assertTrue(futures.get(futures.size() - 1).equals(futures.get(futures.size() - 2)));

        }

        dataLdr.close();
    }

    /**
     * Check that all IgniteFutures that should be streamed are done.
     */
    public void testallFuturesAreDone() {
        for (int i = 1; i <= DATA_AMOUNT; i++)
            futures.add(dataLdr.addData(i, i));

        dataLdr.close();

        for (IgniteFuture future :
                futures)
            assertTrue(future.isDone());
    }

//    /**
//     *удалять имхо должен моментально, так как пока соберется батч, станет уже неактуально
//     */
//    public void testRemoveData(){
//        for (int i = 1; i <= DATA_AMOUNT; i++)
//            dataLdr.removeData(i);
//    }
}
