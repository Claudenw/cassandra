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


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;

public class SettingsPort extends AbstractSettings implements Serializable
{

    public final int nativePort;
    public final int jmxPort;

    private static final String NATIVE = "port-native";
    private static final String JMX = "port-jmx";

    private static final int DEFAULT_NATIVE = 9042;
    private static final int DEFAULT_JMX = 7199;


//    public SettingsPort(PortOptions options)
//    {
//        nativePort = Integer.parseInt(options.nativePort.value());
//        jmxPort = Integer.parseInt(options.jmxPort.value());
//    }

    public SettingsPort(CommandLine cmdLine)
    {
        try {
            nativePort = cmdLine.getParsedOptionValue(NATIVE, DEFAULT_NATIVE);
            jmxPort = cmdLine.getParsedOptionValue(JMX, DEFAULT_JMX);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Options getOptions() {
        Predicate<String> boundsChecker = rangeVerifier(0, Range.inclusive, 65535, Range.inclusive);
        Options result = new Options();
        result.addOption( Option.builder(NATIVE).hasArg(true).desc(format("Use this port for the Cassandra native protocol. (Default %s)", DEFAULT_NATIVE)).type(Integer.class).verifier(boundsChecker).build());
        result.addOption( Option.builder(JMX).hasArg(true).desc(format("Use this port for retrieving statistics over jmx. (Default %s)", DEFAULT_JMX) ).type(Integer.class).verifier(boundsChecker).build());
        return result;
    }

    public static SettingsPort get(CommandLine cmdLine) {
        return new SettingsPort(cmdLine);
    }



//    // Option Declarations
//
//    private static final class PortOptions extends GroupedOptions
//    {
//        final OptionSimple nativePort = new OptionSimple("native=", "[0-9]+", "9042", "Use this port for the Cassandra native protocol", false);
//        final OptionSimple jmxPort = new OptionSimple("jmx=", "[0-9]+", "7199", "Use this port for retrieving statistics over jmx", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(nativePort, jmxPort);
//        }
//    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Native Port: %d%n", nativePort);
        out.printf("  JMX Port: %d%n", jmxPort);
    }


//    public static SettingsPort get(Map<String, String[]> clArgs)
//    {
//        String[] params = clArgs.remove("-port");
//        if (params == null)
//        {
//            return new SettingsPort(new PortOptions());
//        }
//        PortOptions options = GroupedOptions.select(params, new PortOptions());
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -port options provided, see output for valid options");
//            System.exit(1);
//        }
//        return new SettingsPort(options);
//    }

//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-port", new PortOptions());
//    }
//    }
}

