package org.apache.ignite.internal.benchmarks.jmh;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAbstractBenchmark;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

@State(Scope.Benchmark)
public class JmhStreamerAddDataBenchmark extends JmhCacheAbstractBenchmark {
    /** Default cache name. */
    private static final String DEFAULT_CACHE_NAME = "default";

    /** Data loader. */
    private IgniteDataStreamer<Integer, Integer> dataLdr;

    /** Test list. */
    private static Collection<AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();

    /** Data amount. */
    private static final int DATA_AMOUNT = 5;


    /**
     * Create Ignite configuration.
     */
    private static IgniteConfiguration getConfiguration(String cfgName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        if (cfgName.contains("client"))
            cfg.setClientMode(true);

        cfg.setIgniteInstanceName(cfgName);

        return cfg;
    }

    /**
     * @return New cache configuration with modified defaults.
     */
    public static CacheConfiguration defaultCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setEvictionPolicy(null);

        return cfg;
    }


    /**
     * Start 3 servers and 1 client.
     */
    @Setup(Level.Trial)
    public void goSetup() {
        IgniteConfiguration cfgSrv1 = getConfiguration("server1");

        IgniteConfiguration cfgSrv2 = getConfiguration("server2");

        IgniteConfiguration cfgClient = getConfiguration("client");

        Ignition.start(cfgSrv1);

        Ignition.start(cfgSrv2);

        Ignite client = Ignition.start(cfgClient);

        fillCollection();

        dataLdr = client.dataStreamer(cfgClient.getCacheConfiguration()[0].getName());
    }

    private static void fillCollection() {
        for (int i = 0; i < DATA_AMOUNT; i++)
            testList.add(new HashMap.SimpleEntry<>(i, i));
    }

    /**
     * Tear down routine.
     *
     * @throws Exception If failed.
     */
    @TearDown(Level.Trial)
    public void gotearDown() throws Exception {
        Ignition.stopAll(true);
    }


    /**
     * Perfomance of addData per collection.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void addDataCollection() {
        dataLdr.addData(testList);
    }

    /**
     * Perfomance of addData per key value.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void addDataKeyValue() {
        for (int i = 0; i < DATA_AMOUNT; i++)
            dataLdr.addData(i, i);

    }

    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(1))
                .operationsPerInvocation(3)
                .warmupIterations(9)
                .forks(1)
                .threads(1)
                .include(JmhStreamerAddDataBenchmark.class.getSimpleName());

        new Runner(builder.build()).run();
    }
}