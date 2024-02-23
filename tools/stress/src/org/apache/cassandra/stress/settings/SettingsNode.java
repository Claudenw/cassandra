package org.apache.cassandra.stress.settings;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.google.common.net.HostAndPort;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import com.datastax.driver.core.Host;
import org.apache.cassandra.stress.util.ResultLogger;


public class SettingsNode extends AbstractSettings
{
    public static final StressOption<String> NODE_DATACENTER = new StressOption<>(new Option("datacenter", true, "Datacenter used for DCAwareRoundRobinLoadPolicy"));
    public static final StressOption<String> NODE_WHITELIST = new StressOption<>(new Option("whitelist", "Limit communications to the provided nodes"));
    public static final StressOption<FileInputStream> NODE_FILE = new StressOption<>(Option.builder("node-file").desc("Node file (one per line).  May not be used with -node-list.").hasArg().argName("file").type(FileInputStream.class).build());
    public static final StressOption<List<String>> NODE_LIST = new StressOption<>(()->List.of("localhost"),
                                                                                  Option.builder("node-list").desc("A comma delimited list of nodes.  May not be used with -node-file.").hasArg()
                                                                                        .converter(s -> Arrays.asList(s.split(","))).build());
    public final List<String> nodes;
    public final boolean isWhiteList;
    public final String datacenter;
    private final Random random;

    public SettingsNode(CommandLine commandLine)
    {
        this.random = new Random();
        if (commandLine.hasOption(NODE_FILE.option()))
        {
            String node;
            nodes = new ArrayList<>();
            try(BufferedReader in = new BufferedReader(new InputStreamReader(NODE_FILE.extract(commandLine))))
            {
                while ((node = in.readLine()) != null)
                {
                    if (node.length() > 0)
                        nodes.add(node);
                }
            }
            catch(Exception e)
            {
                throw asRuntimeException(e);
            }
        }
        else
        {
            nodes = NODE_LIST.extract(commandLine);
        }
        isWhiteList = commandLine.hasOption(NODE_WHITELIST.option());
        datacenter = NODE_DATACENTER.extract(commandLine);
    }

    public Set<String> resolveAllPermitted(StressSettings settings)
    {
        Set<String> r = new HashSet<>();
        switch (settings.mode.api)
        {
            case JAVA_DRIVER_NATIVE:
                if (!isWhiteList)
                {
                    for (Host host : settings.getJavaDriverClient().getCluster().getMetadata().getAllHosts())
                        r.add(host.getSocketAddress().getHostString() + ":" + host.getSocketAddress().getPort());
                    break;
                }
            case SIMPLE_NATIVE:
                for (InetAddress address : resolveAllSpecified())
                    r.add(address.getHostName());
        }
        return r;
    }

    public Set<InetAddress> resolveAllSpecified()
    {
        Set<InetAddress> r = new HashSet<>();
        for (String node : nodes)
        {
            try
            {
                HostAndPort hap = HostAndPort.fromString(node);
                r.add(InetAddress.getByName(hap.getHost()));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
        return r;
    }

    public Set<InetSocketAddress> resolveAll(int port)
    {
        Set<InetSocketAddress> r = new HashSet<>();
        for (String node : nodes)
        {
            try
            {
                HostAndPort hap = HostAndPort.fromString(node).withDefaultPort(port);
                r.add(new InetSocketAddress(InetAddress.getByName(hap.getHost()), hap.getPort()));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
        return r;
    }

    public String randomNode()
    {
        return nodes.get( random.nextInt(0,nodes.size()));
    }

    // Option Declarations
    public static Options getOptions() {
        return new Options()
               .addOption(NODE_DATACENTER.option())
               .addOption(NODE_WHITELIST.option())
               .addOptionGroup(
               new OptionGroup()
               .addOption(NODE_FILE.option())
               .addOption(NODE_LIST.option()))
        ;
    }


    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.println("  Nodes: " + nodes);
        out.println("  Is White List: " + isWhiteList);
        out.println("  Datacenter: " + PrintUtils.printNull(datacenter));
    }
}
