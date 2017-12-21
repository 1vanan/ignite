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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JmhStreamerAddDataMultiThreadBenchmark extends JmhAbstractBenchmark {
    /**
     * Default cache name.
     */
    private static final String DEFAULT_CACHE_NAME = "default";

    /**
     * Data amount.
     */
    private static final int DATA_AMOUNT = 1000;

    /**
     * Thread amount.
     */
    static final AtomicInteger streamerId = new AtomicInteger(0);

    /**
     * Server 1.
     */
    private Ignite srv1;

    /**
     * Server 2.
     */
    private Ignite srv2;

    /**
     * Client node.
     */
    private static Ignite client;


    /**
     * Create Ignite configuration.
     */
    private static IgniteConfiguration getConfiguration(String cfgName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(cfgName);


        if (isClient) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration(defaultCacheConfiguration(0), defaultCacheConfiguration(1),
                    defaultCacheConfiguration(2));
        } else
            cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /**
     * @return New cache configuration with modified defaults for client node.
     */
    private static CacheConfiguration defaultCacheConfiguration(int cacheNumber) {
        CacheConfiguration cfg;

        cfg = new CacheConfiguration(DEFAULT_CACHE_NAME + cacheNumber);

        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return New cache configuration with modified defaults for server node.
     */
    private static CacheConfiguration defaultCacheConfiguration() {
        CacheConfiguration cfg;

        cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * Start 3 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void setup() {
        srv1 = Ignition.start(getConfiguration("srv1", false));

        srv2 = Ignition.start(getConfiguration("srv2", false));

        client = Ignition.start(getConfiguration("clt", true));
    }

    /**
     * Stop all grids after all tests.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        Ignition.stopAll(true);
    }

    /**
     * Clean caches after each operation.
     */
    @TearDown(Level.Iteration)
    public void clean() {
        srv1.cache(DEFAULT_CACHE_NAME).clear();

        srv2.cache(DEFAULT_CACHE_NAME).clear();
    }

    /**
     * Perfomance of addData per collection.
     */
    @Benchmark
    public void addDataCollection(CollectionStreamer streamer) {
        streamer.run();
    }

    /**
     * Inner class which prepares collection and streams it.
     */
    @State(Scope.Thread)
    public static class CollectionStreamer implements Runnable {
        /**
         * List that will be streaming from client.
         */
        private Collection<AbstractMap.SimpleEntry<Integer, Integer>> streamingList = new ArrayList<>();

        /**
         * Streamer id.
         */
        int id;

        /**
         * Data loader.
         */
        IgniteDataStreamer<Integer, Integer> dataLdr;

        /**
         * Default constructor. Set streamer id and fill streaming collection.
         */
        public CollectionStreamer() {
            this.id = streamerId.getAndIncrement();

            for (int i = 0; i < DATA_AMOUNT; i++)
                streamingList.add(new HashMap.SimpleEntry<>(i, i));

            dataLdr = client.dataStreamer(DEFAULT_CACHE_NAME + id);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            dataLdr.addData(streamingList);
        }
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
                .warmupIterations(5)
                .forks(1)
                .threads(3)
                .include(JmhStreamerAddDataMultiThreadBenchmark.class.getSimpleName());

        new Runner(builder.build()).run();
    }
}
