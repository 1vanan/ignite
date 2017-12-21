package org.apache.ignite.internal.benchmarks.jmh.streamer;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Standart methods for preparing to streaming data.
 */
@State(Scope.Benchmark)
class JmhStreamerAbstractBenchmark {
    /**
     * Default cache name.
     */
    static final String DEFAULT_CACHE_NAME = "default";

    /**
     * Thread amount.
     */
    static final AtomicInteger streamerId = new AtomicInteger(0);

    /**
     * Server 1.
     */
    Ignite srv1;

    /**
     * Server 2.
     */
    Ignite srv2;

    /**
     * Client node.
     */
    static Ignite client;

    /**
     * Create Ignite configuration.
     */
    static IgniteConfiguration getConfiguration(String cfgName, boolean isClient) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(cfgName);

        if (isClient) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration(defaultCacheConfiguration(0), defaultCacheConfiguration(1),
                    defaultCacheConfiguration(2), defaultCacheConfiguration(3),
                    defaultCacheConfiguration(4), defaultCacheConfiguration(5));
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

}
