package org.apache.ignite.internal.benchmarks.jmh;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JmhStreamerAddDataBenchmark extends JmhAbstractBenchmark {
    /**
     * Default cache name.
     */
    private static final String DEFAULT_CACHE_NAME = "default";

    /**
     * Data loader.
     */
    private DataStreamerImpl<Integer, Integer> dataLdr;

    /**
     * Test list.
     */
    private static Collection<AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();

    /**
     * Data amount.
     */
    private static final int DATA_AMOUNT = 1000;

    /** Batch size. */
    private static final int BATCH_SIZE = 1000;

    /** Client. */
    private Ignite srv1;

    /** Server 2. */
    private Ignite srv2;


    /**
     * Create Ignite configuration.
     */
    private static IgniteConfiguration getConfiguration(String cfgName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName(cfgName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        if (isClient)
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @return New cache configuration with modified defaults.
     */
    private static CacheConfiguration defaultCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    @State(Scope.Benchmark)
    public static class StreamingMap{
        static Map<Integer, Integer> intMap = new HashMap<>();
    }


    /**
     * Start 3 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() {
        srv1 = Ignition.start(getConfiguration("srv1", false));
        srv2 = Ignition.start(getConfiguration("srv2", false));
        Ignite client = Ignition.start(getConfiguration("clt", true));

        prepareStreamingCollection();

        dataLdr = (DataStreamerImpl) client.dataStreamer(DEFAULT_CACHE_NAME);

        dataLdr.setBufStreamerSizePerKeyVal(BATCH_SIZE);

        for(int i = 0; i < DATA_AMOUNT; i++)
            StreamingMap.intMap.put(i, i);
    }

    /**
     *Fill streaming collection.
     */
    private static void prepareStreamingCollection() {
        for (int i = 0; i < DATA_AMOUNT; i++)
            testList.add(new HashMap.SimpleEntry<>(i, i));
    }

    /**
     * Stop all grids after all tests.
     */
    @TearDown(Level.Trial)
    public void tearDown(){
        Ignition.stopAll(true);
    }

    /**
     * Clean caches and loader after each operation.
     */
    @TearDown(Level.Iteration)
    public void clean(){
        srv1.cache(DEFAULT_CACHE_NAME).clear();
        srv2.cache(DEFAULT_CACHE_NAME).clear();

        dataLdr.flush();

        dataLdr.clearList();

        dataLdr.clearFuts();
    }

//    /**
//     * Perfomance of addData per collection.
//     */
//    @Benchmark
//    public void addDataCollection() {
//        dataLdr.addData(testList);
//    }

    /**
     * Perfomance of addData per key value.
     */
    @Benchmark
    public void addDataKeyValue(StreamingMap streamingMap) {

        Map<Integer, Integer> data = streamingMap.intMap;

        for (Map.Entry<Integer, Integer> entry : data.entrySet())
            dataLdr.addData(entry.getKey(), entry.getValue());
    }



    /**
     * Launch benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .measurementIterations(20)
                .operationsPerInvocation(3)
                .warmupIterations(7)
                .forks(1)
                .threads(1)
                .include(JmhStreamerAddDataBenchmark.class.getSimpleName());

        new Runner(builder.build()).run();
    }
}