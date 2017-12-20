package org.apache.ignite.internal.benchmarks.jmh;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class JmhStreamerAddDataMultiThreadBenchmark extends JmhAbstractBenchmark {
    /**
     * Default cache name.
     */
    private static final String DEFAULT_CACHE_NAME = "default";

    /**
     * Test list.
     */
    private static Collection<AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();

    /**
     * Data amount.
     */
    private static final int DATA_AMOUNT = 10;

    /**
     * Thread amount.
     */
    private static final int THREAD_AMOUNT = 1;

    /**
     * Executor.
     */
    private ThreadFactory executor;

    /**
     * Client.
     */
    private Ignite srv1;

    /**
     * Server 2.
     */
    private Ignite srv2;

    /** Client. */
    private static Ignite client;

    private static List<IgniteDataStreamer<Integer, Integer>> dataLdrList = new ArrayList<>();


    /**
     * Create Ignite configuration.
     */
    private static IgniteConfiguration getConfiguration(String cfgName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteInstanceName(cfgName);


        if (isClient) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration(defaultCacheConfiguration(0), defaultCacheConfiguration(1));
        } else
            cfg.setCacheConfiguration(defaultCacheConfiguration(111));

        return cfg;
    }

    /**
     * @return New cache configuration with modified defaults.
     */
    private static CacheConfiguration defaultCacheConfiguration(int cacheNumber) {
        CacheConfiguration cfg;

        if (cacheNumber == 111)
            cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        else
            cfg = new CacheConfiguration(DEFAULT_CACHE_NAME + cacheNumber);

        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    @State(Scope.Benchmark)
    public static class StreamingMap {
        static Map<Integer, Integer> intMap = new HashMap<>();
    }


    /**
     * Start 3 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() {
        srv1 = Ignition.start(getConfiguration("srv1", false));
//        srv2 = Ignition.start(getConfiguration("srv2", false));
        client = Ignition.start(getConfiguration("clt", true));

        prepareStreamingCollection(client);

        for (int i = 0; i < DATA_AMOUNT; i++)
            StreamingMap.intMap.put(i, i);

        executor = Executors.defaultThreadFactory();
    }

    /**
     * Fill streaming collection.
     */
    private static void prepareStreamingCollection(Ignite client) {
        for (int i = 0; i < DATA_AMOUNT; i++)
            testList.add(new HashMap.SimpleEntry<>(i, i));

        for (int i = 0; i < THREAD_AMOUNT; i++)
            dataLdrList.add(client.<Integer, Integer>dataStreamer(DEFAULT_CACHE_NAME + i));

    }

    /**
     * Stop all grids after all tests.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * Clean caches and loader after each operation.
     */
    @TearDown(Level.Iteration)
    public void clean() {
        srv1.cache(DEFAULT_CACHE_NAME).clear();
//        srv2.cache(DEFAULT_CACHE_NAME).clear();
//        for (IgniteDataStreamer dataLdr : dataLdrList)
//            dataLdr.flush();

    }

    /**
     * Perfomance of addData per collection.
     */
    @Benchmark
    public void addDataCollection(CollectionStreamer streamer) {
        for (int i = 0; i < THREAD_AMOUNT; i++) {
            streamer.setThreadId(i);

            Thread thread = executor.newThread(streamer);

            thread.start();
        }
    }

    /**
     *
     */
    @State(Scope.Benchmark)
    public static class CollectionStreamer implements Runnable {

        int threadId;

        public void setThreadId(int threadId) {
            this.threadId = threadId;
        }

        @Override
        public void run() {
            IgniteDataStreamer<Integer, Integer> dataLdr = client.dataStreamer(DEFAULT_CACHE_NAME + threadId);

            dataLdr.addData(testList);
        }
    }

    /**
     * Launch benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .measurementIterations(10)
                .operationsPerInvocation(3)
                .warmupIterations(5)
                .forks(1)
                .threads(1)
                .include(JmhStreamerAddDataMultiThreadBenchmark.class.getSimpleName());

        new Runner(builder.build()).run();
    }
}
