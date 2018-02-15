/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public class JmhStreamerKeyValueBenchmark extends JmhStreamerAbstractBenchmark {
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
    public void addData(KeyValueStreamer dataStreamer) {
        for (Map.Entry<Integer, Integer> entry : dataStreamer.data.entrySet())
            dataStreamer.dataLdr.addData(entry.getKey(), entry.getValue());
    }

    /**
     * Inner class which prepares collection and streams it.
     */
    @State(Scope.Thread)
    public static class KeyValueStreamer {
        /**
         * List that will be streaming from client.
         */
        private Map<Integer, Integer> data = new HashMap<>();

        /**
         * Data loader.
         */
        private IgniteDataStreamer<Integer, Integer> dataLdr;

        /**
         * Default constructor. Set streamer id and fill collection with streaming data.
         */
        public KeyValueStreamer() {
            for (int i = 0; i < DATA_AMOUNT; i++)
                data.put(i, i);

            dataLdr = client.dataStreamer(DEFAULT_CACHE_NAME + "client");
        }
    }

    /**
     * Launch benchmark.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .measurementIterations(21)
                .operationsPerInvocation(3)
                .warmupIterations(7)
                .forks(1)
                .threads(3)
                .include(JmhStreamerKeyValueBenchmark.class.getSimpleName());

        new Runner(builder.build()).run();
    }
}