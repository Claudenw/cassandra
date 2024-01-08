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

import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Option;

public class SettingsRate extends AbstractSettings implements Serializable
{

    public final boolean auto;
    public final int minThreads;
    public final int maxThreads;
    public final int threadCount;
    public final int opsPerSecond;
    public final boolean isFixed;

//    public SettingsRate(ThreadOptions options)
//    {
//        auto = false;
//        threadCount = Integer.parseInt(options.threads.value());
//        String throttleOpt = options.throttle.value();
//        String fixedOpt = options.fixed.value();
//        int throttle = Integer.parseInt(throttleOpt.substring(0, throttleOpt.length() - 2));
//        int fixed = Integer.parseInt(fixedOpt.substring(0, fixedOpt.length() - 2));
//        if(throttle != 0 && fixed != 0)
//            throw new IllegalArgumentException("can't have both fixed and throttle set, choose one.");
//        opsPerSecond = Math.max(fixed, throttle);
//        isFixed = (opsPerSecond == fixed);
//
//        minThreads = -1;
//        maxThreads = -1;
//    }
//
//    public SettingsRate(AutoOptions auto)
//    {
//        this.auto = auto.auto.setByUser();
//        this.minThreads = Integer.parseInt(auto.minThreads.value());
//        this.maxThreads = Integer.parseInt(auto.maxThreads.value());
//        this.threadCount = -1;
//        this.opsPerSecond = 0;
//        isFixed = false;
//    }

    public SettingsRate(CommandLine cmdLine) {
        this.auto = cmdLine.hasOption(AUTO);
        this.isFixed = cmdLine.hasOption(FIXED);
        try {
            if (this.auto) {
                this.minThreads = cmdLine.getParsedOptionValue(MIN_CLIENTS, MIN_CLIENTS_DEFAULT);
                this.maxThreads = cmdLine.getParsedOptionValue(MAX_CLIENTS, MAX_CLIENTS_DEFAULT);
                this.threadCount = IGNORE;
                this.opsPerSecond = THROTTLE_DEFAULT;
            } else {
                this.threadCount = cmdLine.getParsedOptionValue(CLIENTS);
                this.opsPerSecond = isFixed ? cmdLine.getParsedOptionValue(FIXED) : cmdLine.getParsedOptionValue(THROTTLE, THROTTLE_DEFAULT);
                this.minThreads = IGNORE;
                this.maxThreads = IGNORE;
            }
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }
    }

    private SettingsRate(int rate) {
        this.auto = false;
        this.isFixed = true;
                this.threadCount = rate;
                this.opsPerSecond = THROTTLE_DEFAULT;
                this.minThreads = IGNORE;
                this.maxThreads = IGNORE;
    }

    public SettingsRate() {
        this.auto = true;
        this.isFixed = false;
        this.threadCount = IGNORE;
        this.opsPerSecond = THROTTLE_DEFAULT;
        this.minThreads = MIN_CLIENTS_DEFAULT;
        this.maxThreads = MAX_CLIENTS_DEFAULT;
    }

    private static final int IGNORE = -1;

    private static final String AUTO = "-rate-auto";
    private static final String MIN_CLIENTS = "-rate-min-clients";
    private static final int MIN_CLIENTS_DEFAULT = 4;
    private static final String MAX_CLIENTS = "-rate-max-clients";
    private static final int MAX_CLIENTS_DEFAULT = 1000;
    private static final String FIXED = "-rate-fixed";
    private static final String THROTTLE = "-rate-throttle";
    private static final int THROTTLE_DEFAULT = 0;

    private static final String CLIENTS = "-rate-clients";

    private static OptionGroup AUTO_OR_THREADS;


    // Option Declarations

//    private static final class AutoOptions extends GroupedOptions
//    {
//        final OptionSimple auto = new OptionSimple("auto", "", null, "stop increasing threads once throughput saturates", false);
//        final OptionSimple minThreads = new OptionSimple("threads>=", "[0-9]+", "4", "run at least this many clients concurrently", false);
//        final OptionSimple maxThreads = new OptionSimple("threads<=", "[0-9]+", "1000", "run at most this many clients concurrently", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(minThreads, maxThreads, auto);
//        }
//    }
//
//    private static final class ThreadOptions extends GroupedOptions
//    {
//        final OptionSimple threads = new OptionSimple("threads=", "[0-9]+", null, "run this many clients concurrently", true);
//        final OptionSimple throttle = new OptionSimple("throttle=", "[0-9]+/s", "0/s", "throttle operations per second across all clients to a maximum rate (or less) with no implied schedule", false);
//        final OptionSimple fixed = new OptionSimple("fixed=", "[0-9]+/s", "0/s", "expect fixed rate of operations per second across all clients with implied schedule", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(threads, throttle, fixed);
//        }
//    }

    public static Options getOptions() {
        Options result = new Options();
        AUTO_OR_THREADS = new OptionGroup()
                .addOption(new Option(AUTO, "stop increasing threads once throughput saturates"))
                .addOption(Option.builder(FIXED).hasArg(true).desc("run this many clients concurrently" ).type(Integer.class).build())
                .addOption(Option.builder(THROTTLE).hasArg(true).desc("throttle operations per second across all clients to a maximum rate (or less) with no implied schedule.").type(Integer.class).build());
        result.addOptionGroup(AUTO_OR_THREADS);
        result.addOption(Option.builder(MIN_CLIENTS).hasArg(true).desc("run at least this many clients concurrently.  (Only valid with "+AUTO+")").type(Integer.class).build());
        result.addOption(Option.builder(MAX_CLIENTS).hasArg(true).desc("run at most this many clients concurrently.  (Only valid with "+AUTO+")").type(Integer.class).build());
        result.addOption(Option.builder(CLIENTS).hasArg(true).desc("run this many clients concurrently.  (Only valid with "+FIXED+" or "+THROTTLE+")").type(Integer.class).build());
        return result;
    }

    // CLI Utility Methods

    public void printSettings(ResultLogger out)
    {
        out.printf("  Auto: %b%n", auto);
        if (auto)
        {
            out.printf("  Min Threads: %d%n", minThreads);
            out.printf("  Max Threads: %d%n", maxThreads);
        } else {
            out.printf("  Thread Count: %d%n", threadCount);
            out.printf("  OpsPer Sec: %d%n", opsPerSecond);
        }
    }

    public static SettingsRate get(CommandLine cmdLine, SettingsCommand command)
    {
        if (AUTO_OR_THREADS.getSelected() == null)
        {
            switch (command.type)
            {
                case WRITE:
                case COUNTER_WRITE:
                    if (command.count > 0)
                    {
//
//                        ThreadOptions options = new ThreadOptions();
//                        options.accept("threads=200");
                        return new SettingsRate(200);
                    }
            }
            return new SettingsRate();
        }
        return new SettingsRate(cmdLine);
    }

//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-rate", new ThreadOptions(), new AutoOptions());
//    }
//
//    public static Runnable helpPrinter()
//    {
//        return SettingsRate::printHelp;
//    }
}

