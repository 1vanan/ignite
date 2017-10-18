package org.apache.ignite.spi.discovery.tcp.ipfinder.vm;

import java.util.Arrays;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;

/**
 * Test printing warning in setAddresses when there is at least one wrong ip address.
 */
public class TcpDiscoveryVmIpFinderSetAddressesWarningTest
        extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryVmIpFinder> {

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    protected TcpDiscoveryVmIpFinderSetAddressesWarningTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryVmIpFinder ipFinder() {
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();

        assert !finder.isShared() : "Ip finder should NOT be shared by default.";

        return finder;
    }

    /**
     * Fail if execution time of setAddresses is more than TCP timeout on Windows OS when one or more wrong address was
     * added.
     *
     * @throws Exception If any error occurs.
     */
    public void testWrongIpAddressesSetting() throws Exception {
        boolean wrongIpAddrWasAdded;

        long executionTime;
        long winTcpTimeout = 2200;

        long timeBefore = System.currentTimeMillis();

        finder.setAddresses(Arrays.asList("[::1]:45555", "8.8.8.8", "527.0.0.1", "some-dns-name1:200"));

        executionTime = System.currentTimeMillis() - timeBefore;

        wrongIpAddrWasAdded = executionTime >= winTcpTimeout;

        assertTrue(wrongIpAddrWasAdded);
    }
}