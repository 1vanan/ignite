package org.apache.ignite.internal.benchmarks.jmh;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
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

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

@State(Scope.Benchmark)
public class JmhStreamerAddDataBenchmark extends JmhAbstractBenchmark {
    /**
     * Property: backups.
     */
    protected static final String PROP_BACKUPS = "ignite.jmh.cache.backups";

    /**
     * Property: atomicity mode.
     */
    protected static final String PROP_ATOMICITY_MODE = "ignite.jmh.cache.atomicityMode";

    /**
     * Property: atomicity mode.
     */
    protected static final String PROP_WRITE_SYNC_MODE = "ignite.jmh.cache.writeSynchronizationMode";

    /**
     * Property: nodes count.
     */
    protected static final String PROP_DATA_NODES = "ignite.jmh.cache.dataNodes";

    /**
     * Property: client mode flag.
     */
    protected static final String PROP_CLIENT_MODE = "ignite.jmh.cache.clientMode";

    /**
     * Default amount of nodes.
     */
    protected static final int DFLT_DATA_NODES = 1;

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * Client node.
     */
    protected Ignite clientNode;

    /**
     * First server node.
     */
    protected Ignite svrNode1;

    /**
     * Server server node.
     */
    protected Ignite svrNode2;

    /**
     * Target cache.
     */
    protected IgniteCache cache;

    /**
     * Test list.
     */
    private Collection<AbstractMap.SimpleEntry<Integer, Integer>> testList = new ArrayList<>();


    /**
     * Ignite data streamer.
     */
    private static IgniteDataStreamer<Integer, Integer> dataLdr;
    private String DEFAULT_CACHE_NAME = "default";

    /**
     *
     */
    @Setup
    public void setup() {
//        CacheConfiguration ccfg = new CacheConfiguration("default");

//        ccfg.setAtomicityMode(TRANSACTIONAL);
//        ccfg.setNearConfiguration(new NearCacheConfiguration());
//        ccfg.setWriteSynchronizationMode(FULL_SYNC);
//        ccfg.setEvictionPolicy(null);

//        IgniteConfiguration cfgClient = new IgniteConfiguration();

        IgniteConfiguration cfgSrv = new IgniteConfiguration();

        IgniteConfiguration cfgSrv2 = new IgniteConfiguration();

//
//        TcpDiscoverySpi spiSrv = (TcpDiscoverySpi) cfgSrv.getDiscoverySpi();
//
//        Collection collection = new ArrayList();
//
//        collection.add("127.0.0.1:47501");
//
//        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);
//
//        ipFinder.setAddresses(collection);
//
//        spiSrv.setIpFinder(ipFinder);
//
//        ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));
//
//        cfgClient.setCacheConfiguration(ccfg);
//
//        cfgSrv.setCacheConfiguration(ccfg);
//
//        cfgClient.setClientMode(true);
//
//
//
//        clientNode = Ignition.start(cfgClient);

        svrNode1 = Ignition.start(cfgSrv);

        svrNode2 = Ignition.start(cfgSrv2);

//        dataLdr = clientNode.dataStreamer(ccfg.getName());

    }

    /**
     *
     */
    @Benchmark
    public void addDataCollection() {
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
        Ignition.stopAll(true);
    }

    private static IgniteConfiguration getConfiguration(String instanceName){
        IgniteConfiguration cfg = new IgniteConfiguration();

        if(instanceName.contains("client"))
            cfg.setClientMode(true);

        cfg.setIgniteInstanceName(instanceName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(DEFAULT_CACHE_NAME);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        // Set atomicity mode.
        CacheAtomicityMode atomicityMode = enumProperty(PROP_ATOMICITY_MODE, CacheAtomicityMode.class);

        if (atomicityMode != null)
            cacheCfg.setAtomicityMode(atomicityMode);

        // Set write synchronization mode.
        CacheWriteSynchronizationMode writeSyncMode =
                enumProperty(PROP_WRITE_SYNC_MODE, CacheWriteSynchronizationMode.class);

        if (writeSyncMode != null)
            cacheCfg.setWriteSynchronizationMode(writeSyncMode);

        // Set backups.
        cacheCfg.setBackups(intProperty(PROP_BACKUPS));

        return cacheCfg;
    }

    public static void main(String[] args) throws RunnerException {
        ChainedOptionsBuilder builder = new OptionsBuilder()
                .timeUnit(TimeUnit.NANOSECONDS)
                .measurementIterations(1)
                .measurementTime(TimeValue.seconds(1))
                .warmupIterations(1)
                .forks(1)
                .threads(10)
                .mode(Mode.AverageTime)
                .include(JmhStreamerAddDataBenchmark.class.getSimpleName());

        new Runner(builder.build()).run();
    }
}