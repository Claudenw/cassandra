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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.net.HostAndPort;
import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;


import com.datastax.driver.core.Host;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

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
        logger.assertContains("Nodes: [localhost]");
        logger.assertContains("Is White List: false");
        logger.assertContains("Datacenter: *not set*");
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
        logger.assertContains("Nodes: [localhost]");
        logger.assertContains("Is White List: false");
        logger.assertContains("Datacenter: dc1");
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
        logger.assertContains("Nodes: [localhost]");
        logger.assertContains("Is White List: true");
        logger.assertContains("Datacenter: *not set*");
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
        logger.assertContains("Nodes: [one, two, three]");
        logger.assertContains("Is White List: false");
        logger.assertContains("Datacenter: *not set*");
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
        logger.assertContains("Nodes: [four, five, six]");
        logger.assertContains("Is White List: false");
        logger.assertContains("Datacenter: *not set*");
    }

    @Test
    public void resolveAllPermittedTest() throws ParseException
    {
        fail("not implemented");
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
