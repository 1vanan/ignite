package org.apache.ignite.internal.benchmarks.jmh;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class JmhStreamerAddDataBenchmark extends JmhAbstractBenchmark {
    /** Property: backups. */
    protected static final String PROP_BACKUPS = "ignite.jmh.cache.backups";

    /** Property: atomicity mode. */
    protected static final String PROP_ATOMICITY_MODE = "ignite.jmh.cache.atomicityMode";

    /** Property: atomicity mode. */
    protected static final String PROP_WRITE_SYNC_MODE = "ignite.jmh.cache.writeSynchronizationMode";

    /** Property: nodes count. */
    protected static final String PROP_DATA_NODES = "ignite.jmh.cache.dataNodes";

    /** Property: client mode flag. */
    protected static final String PROP_CLIENT_MODE = "ignite.jmh.cache.clientMode";

    /** Default amount of nodes. */
    protected static final int DFLT_DATA_NODES = 1;

    /** Client node. */
    protected Ignite clientNode;

    /** First server node. */
    protected Ignite svrNode1;

    /** Server server node. */
    protected Ignite svrNode2;

    /** Target cache. */
    protected IgniteCache cache;

    /** Test list. */
    private Collection<AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();


    /** Ignite data streamer. */
    private static IgniteDataStreamer<Integer, Integer> dataLdr;

    /**
     *
     */
    @Setup
    public void setup(){
//        CacheConfiguration ccfg = new CacheConfiguration("default");
//
//        ccfg.setAtomicityMode(TRANSACTIONAL);
//        ccfg.setNearConfiguration(new NearCacheConfiguration());
//        ccfg.setWriteSynchronizationMode(FULL_SYNC);
//        ccfg.setEvictionPolicy(null);
//
//        IgniteConfiguration cfgClient = new IgniteConfiguration();
//
//        IgniteConfiguration cfgSrv = new IgniteConfiguration();
//
//        ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));
//
//        cfgClient.setCacheConfiguration(ccfg);
//
//        cfgSrv.setCacheConfiguration(ccfg);
//
//        cfgClient.setClientMode(true);
//
//        clientNode = Ignition.start(cfgClient);
//
//        svrNode1 = Ignition.start(cfgSrv);
//
//        svrNode2 = Ignition.start(cfgSrv);
//
//        dataLdr = clientNode.dataStreamer(ccfg.getName());

    }
    /**
     *
     */
    @Benchmark
    public void addDataCollection(){
        System.out.println(123);
//        dataLdr.addData(testList);
    }


    /**
     * Tear down routine.
     *
     * @throws Exception If failed.
     */
    @TearDown
    public void tearDown() throws Exception {
//        Ignition.stopAll(true);
    }

    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
//                .jvmArgs()
                .timeUnit(TimeUnit.MILLISECONDS)
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(20))
//                .warmupIterations(5)
//                .warmupTime(TimeValue.seconds(10))
                .forks(1)
                .threads(1)
                .mode(Mode.Throughput)
                .include(JmhStreamerAddDataBenchmark.class.getSimpleName());

        Collection<RunResult> runResult = new Runner(builder.build()).run();
        for (RunResult r:
             runResult)
            System.out.println("RESULT: " + r.getPrimaryResult().toString());

    }
}
