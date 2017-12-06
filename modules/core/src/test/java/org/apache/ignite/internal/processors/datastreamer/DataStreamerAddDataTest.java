package org.apache.ignite.internal.processors.datastreamer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class DataStreamerAddDataTest extends GridCommonAbstractTest {
    /** Config. */
    private IgniteConfiguration cfg;

    /** Time before. */
    private long timeBefore;

    /** Time after. */
    private long timeAfter;

    /** Entry amount. */
    private static final Integer ENTRY_AMOUNT = 5000;

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

    /** Server 3. */
    private static Ignite srv3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv1 = startGrid("server1");

        srv2 = startGrid("server2");

        srv3 = startGrid("server3");

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

        if (igniteInstanceName.equals("client"))
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        cfg.setGridLogger(strLog);

        return cfg;
    }

    /**
     *
     */
    public void testAddDataKeyValue() throws Exception {
        timeBefore = U.currentTimeMillis();

        for (int i = 0; i < ENTRY_AMOUNT; i++)
            dataLdr.addData(0, 0);

        timeAfter = U.currentTimeMillis();

        System.out.println("Key/Value: " + (timeAfter - timeBefore));
    }

    /**
     *
     */
    public void testAddDataCollection() throws Exception {
        Collection<java.util.AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();

        for (int i = 0; i < ENTRY_AMOUNT; i++)
            testList.add(new HashMap.SimpleEntry<>(i, i));

        timeBefore = U.currentTimeMillis();

        dataLdr.addData(testList);

        timeAfter = U.currentTimeMillis();

        System.out.println("Collection: " + (timeAfter - timeBefore));
    }
}