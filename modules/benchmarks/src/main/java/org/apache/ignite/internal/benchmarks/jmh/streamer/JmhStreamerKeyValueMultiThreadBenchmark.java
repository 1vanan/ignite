package org.apache.ignite.internal.benchmarks.jmh.streamer;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark on streaming key/value.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class JmhStreamerKeyValueMultiThreadBenchmark extends JmhStreamerAbstractBenchmark {
    /**
     * Streaming data amount.
     */
    private final static int DATA_AMOUNT = 1000;

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
     * Perfomance of addData per key/value.
     */
    @Benchmark
    public void addDataCollection(KeyValueStreamer streamer) {
        streamer.run();
    }

    /**
     * Inner class which prepares collection and streams it.
     */
    @State(Scope.Thread)
    public static class KeyValueStreamer implements Runnable {
        /**
         * List that will be streaming from client.
         */
        private Map<Integer, Integer> data = new HashMap<>();

        /**
         * Streamer id.
         */
        private int id;

        /**
         * Data loader.
         */
        private IgniteDataStreamer<Integer, Integer> dataLdr;

        /**
         * Default constructor. Set streamer id and fill streaming collection.
         */
        public KeyValueStreamer() {
            this.id = streamerId.getAndIncrement();

            for (int i = 0; i < DATA_AMOUNT; i++)
                data.put(i, i);

            dataLdr = client.dataStreamer(DEFAULT_CACHE_NAME + id);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            for (Map.Entry<Integer, Integer> entry : data.entrySet())
                dataLdr.addData(entry.getKey(), entry.getValue());
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
                .warmupIterations(7)
                .forks(1)
                .threads(2)
                .include(JmhStreamerKeyValueMultiThreadBenchmark.class.getSimpleName());

        new Runner(builder.build()).run();
    }

}
