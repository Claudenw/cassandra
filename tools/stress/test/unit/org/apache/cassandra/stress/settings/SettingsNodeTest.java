/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.stress.settings;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.stress.util.JavaDriverClient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SettingsNodeTest
{
    @Test
    public void defaultsTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        assertNull(underTest.datacenter);
        assertEquals(List.of("localhost"), underTest.nodes);
        assertFalse(underTest.isWhiteList);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Nodes: [localhost]");
        logger.assertEndsWith("Is White List: false");
        logger.assertEndsWith("Datacenter: *not set*");
    }

    @Test
    public void datacenterTest() throws ParseException
    {
        String[] args = {"-datacenter", "dc1"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        assertEquals("dc1", underTest.datacenter);
        assertEquals(List.of("localhost"), underTest.nodes);
        assertFalse(underTest.isWhiteList);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Nodes: [localhost]");
        logger.assertEndsWith("Is White List: false");
        logger.assertEndsWith("Datacenter: dc1");
    }

    @Test
    public void whitelistTest() throws ParseException
    {
        String[] args = {"-whitelist"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        assertNull(underTest.datacenter);
        assertEquals(List.of("localhost"), underTest.nodes);
        assertTrue(underTest.isWhiteList);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Nodes: [localhost]");
        logger.assertEndsWith("Is White List: true");
        logger.assertEndsWith("Datacenter: *not set*");
    }

    @Test
    public void nodeListTest() throws ParseException
    {
        String[] args = {"-node-list", "one,two,three"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        assertNull(underTest.datacenter);
        assertEquals(List.of("one", "two", "three"), underTest.nodes);
        assertFalse(underTest.isWhiteList);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Nodes: [one, two, three]");
        logger.assertEndsWith("Is White List: false");
        logger.assertEndsWith("Datacenter: *not set*");
    }

    @Test
    public void nodeListNodeFileTest() throws ParseException
    {
        String[] args = {"-node-list", "one,two,three", "-node-file", "someFile"};
        try
        {
            CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected)
        {
            // do nothing
        }
    }

    @Test
    public void noteFileTest() throws ParseException, IOException
    {
        File tempFile = FileUtils.createTempFile("cassandra-stress-node-test", "txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile.toJavaIOFile()))) {
            writer.write("four");
            writer.newLine();
            writer.write("five");
            writer.newLine();
            writer.write("six");
            writer.newLine();
        }
        String[] args = {"-node-file", tempFile.toString()};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        assertNull(underTest.datacenter);
        assertEquals(List.of("four", "five", "six"), underTest.nodes);
        assertFalse(underTest.isWhiteList);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Nodes: [four, five, six]");
        logger.assertEndsWith("Is White List: false");
        logger.assertEndsWith("Datacenter: *not set*");
    }

    @Test
    public void resolveAllPermittedTest() throws ParseException
    {
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), new String[0]);
        SettingsNode underTest = new SettingsNode(commandLine);

        Set<String> expected = underTest.resolveAllSpecified().stream().map(InetAddress::getHostName).collect(Collectors.toSet());

        // test simple native driver.
        StressSettings stressSettings = new StressSettings(new String[] {"READ", "-simple-native"});
        assertEquals(expected, underTest.resolveAllPermitted(stressSettings));


        // test java driver - is not whitelist.
        StressSettingsMockJavaDriver mockedStress = new StressSettingsMockJavaDriver( "READ");
        Cluster mockCluster = mock(Cluster.class);
        Metadata mockMetadata = mock(Metadata.class);
        Host mockHost = mock(Host.class);
        InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 1234);
        when(mockedStress.mockDriver.getCluster()).thenReturn(mockCluster);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);
        when(mockMetadata.getAllHosts()).thenReturn(Set.of(mockHost));
        when(mockHost.getSocketAddress()).thenReturn(address);

        assertEquals(Set.of("localhost:1234"), underTest.resolveAllPermitted(mockedStress));


        // test java driver -- is whitelist
        commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), new String[]{"-whitelist"});
        underTest = new SettingsNode(commandLine);
        assertEquals(expected, underTest.resolveAllPermitted(mockedStress));
    }

    private class StressSettingsMockJavaDriver extends StressSettings
    {
        public JavaDriverClient mockDriver = mock(JavaDriverClient.class);
        public StressSettingsMockJavaDriver(String... args) throws ParseException
        {
            super(args);
        }

        @Override
        public JavaDriverClient getJavaDriverClient(String keyspace)
        {
            return mockDriver;
        }
    }
//
//        // default settings
//        String[] args = {};
//        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
//        SettingsCredentials settingsCredentials = new SettingsCredentials(commandLine);
//        SettingsMode settingsMode = new SettingsMode(commandLine, settingsCredentials);
//        SettingsNode underTest = new SettingsNode(commandLine);
//
//        Set<String> result = underTest.resolveAllPermitted(settingsMode);
//
//
//    Set<String> r = new HashSet<>();
//    switch (settings.mode.api)
//    {
//        case JAVA_DRIVER_NATIVE:
//            if (!isWhiteList)
//            {
//                for (Host host : settings.getJavaDriverClient().getCluster().getMetadata().getAllHosts())
//                    r.add(host.getSocketAddress().getHostString() + ":" + host.getSocketAddress().getPort());
//                break;
//            }
//        case SIMPLE_NATIVE:
//            for (InetAddress address : resolveAllSpecified())
//                r.add(address.getHostName());
//    }
//    return r;
//}

    @Test
    public void resolveAllSpecifiedTest() throws ParseException, UnknownHostException
    {
        // default
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        Set<InetAddress> addresses = underTest.resolveAllSpecified();
        assertEquals(1, addresses.size());
        assertEquals(InetAddress.getByName("localhost"), addresses.iterator().next());

        // with nodes specified
        InetAddress apache = InetAddress.getByName("apache.org");
        InetAddress google = InetAddress.getByName("google.com");
        args = new String[] {"-node-list", apache.getHostAddress()+","+google.getHostAddress()};
        commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        underTest = new SettingsNode(commandLine);
        addresses = underTest.resolveAllSpecified();
        assertEquals(2, addresses.size());
        assertTrue(addresses.contains(google));
        assertTrue(addresses.contains(apache));
    }

    @Test
    public void resolveAllTest() throws ParseException, UnknownHostException
    {
        // default
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        Set<InetSocketAddress> addresses = underTest.resolveAll(4096);
        assertEquals(1, addresses.size());
        assertEquals(new InetSocketAddress(InetAddress.getByName("localhost"), 4096), addresses.iterator().next());

        // with nodes specified
        InetAddress apache = InetAddress.getByName("apache.org");
        InetAddress google = InetAddress.getByName("google.com");
        args = new String[] {"-node-list", apache.getHostAddress()+","+google.getHostAddress()};
        commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        underTest = new SettingsNode(commandLine);
        addresses = underTest.resolveAll(4096);
        assertEquals(2, addresses.size());
        assertTrue(addresses.contains(new InetSocketAddress(google, 4096)));
        assertTrue(addresses.contains(new InetSocketAddress(apache, 4096)));
    }

    @Test
    public void randomNodeTest() throws ParseException
    {
        String[] args = {"-node-list", "one,two,three"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsNode.getOptions(), args);
        SettingsNode underTest = new SettingsNode(commandLine);
        Map<String,Boolean> foundMap = new HashMap<>();

        for (int i=0;i<10000;i++) {
            String node = underTest.randomNode();
            foundMap.put(node,Boolean.TRUE);
        }

        assertTrue(foundMap.get("one"));
        assertTrue(foundMap.get("two"));
        assertTrue(foundMap.get("three"));
    }

}
