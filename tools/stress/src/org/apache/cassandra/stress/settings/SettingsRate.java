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

import static java.lang.String.format;

public class SettingsRate extends AbstractSettings implements Serializable
{

    private static final int IGNORE = -1;

    public static final StressOption<String> RATE_AUTO = new StressOption<>(new Option("rate-auto", "Stop increasing threads once throughput saturates. (Not valid with -rate-fixed or -rate-throttle)"));

    private static final int RATE_MIN_CLIENTS_DEFAULT = 4;
    public static final StressOption<Integer> RATE_MIN_CLIENTS = new StressOption<>(()->RATE_MIN_CLIENTS_DEFAULT,
                                                                                    POSITIVE_VERIFIER,
                                                                                    Option.builder("rate-min-clients").hasArg().type(Integer.class)
                                                                                          .desc(format("Run at least this many clients concurrently.  (Only valid with %s) (Default %s)", RATE_AUTO.key(), RATE_MIN_CLIENTS_DEFAULT)).build());

    private static final int RATE_MAX_CLIENTS_DEFAULT = 1000;
    public static final StressOption<Integer> RATE_MAX_CLIENTS = new StressOption<>(()->RATE_MAX_CLIENTS_DEFAULT,
                                                                                    POSITIVE_VERIFIER,
                                                                                    Option.builder("rate-max-clients").hasArg()
                                                                                          .desc(format("Run at most this many clients concurrently.  (Only valid with %s) (Default %s)", RATE_AUTO.key(), RATE_MAX_CLIENTS_DEFAULT))
                                                                                          .type(Integer.class).build());
    public static final StressOption<Integer> RATE_FIXED = new StressOption(POSITIVE_VERIFIER,
                                                                            Option.builder("rate-fixed").type(Integer.class).hasArg()
                                                                                  .desc("Maximum operations per second. (Not valid with -rate-auto or -rate-throttle)").build());
    private static final int RATE_THROTTLE_DEFAULT = 0;
    public static final StressOption<Integer> RATE_THROTTLE = new StressOption<>(()->RATE_THROTTLE_DEFAULT,
                                                                                 POSITIVE_VERIFIER,
                                                                                 Option.builder("rate-throttle").type(Integer.class).hasArg()
                                                                                       .desc(format("Throttle operations per second across all clients to a maximum rate (or less) with no implied schedule. (Not valid with -%s or -%s) (Default: %s).",
                                                                                                    RATE_FIXED.key(), RATE_AUTO.key(), RATE_THROTTLE_DEFAULT)).build());

    private static final int RATE_CLIENTS_DEFAULT = 0;
    public static final StressOption<Integer> RATE_CLIENTS = new StressOption<>(()->RATE_CLIENTS_DEFAULT,
                                                                                POSITIVE_VERIFIER,
                                                                                Option.builder("rate-clients").hasArg().type(Integer.class)
                                                                                      .desc(format("Run this many clients concurrently.  (Only valid with -%s or -%s) (Default: %s)", RATE_FIXED.key(), RATE_THROTTLE.key(), RATE_CLIENTS_DEFAULT)).build());

    private static OptionGroup AUTO_OR_THREADS = new OptionGroup()
                                                 .addOption(RATE_AUTO.option())
                                                 .addOption(RATE_FIXED.option())
                                                 .addOption(RATE_THROTTLE.option());
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

    private static boolean overrideThreadCount(SettingsCommand command) {
        if (AUTO_OR_THREADS.getSelected() == null)
        {
            switch (command.type)
            {
                case WRITE:
                case COUNTER_WRITE:
                    return command.count > 0;
            }
        }
        return false;
    }

    public SettingsRate(CommandLine cmdLine, SettingsCommand command)
    {
        this.auto = cmdLine.hasOption(RATE_AUTO.option());
        this.isFixed = cmdLine.hasOption(RATE_FIXED.option());
        if (this.auto)
        {
            this.minThreads = RATE_MIN_CLIENTS.extract(cmdLine);
            this.maxThreads = RATE_MAX_CLIENTS.extract(cmdLine);
            this.threadCount = IGNORE;
            this.opsPerSecond = 0;
        }
        else
        {
            this.threadCount = overrideThreadCount(command) ? 200 :RATE_CLIENTS.extract(cmdLine);
            this.opsPerSecond = isFixed ? RATE_FIXED.extract(cmdLine) : RATE_THROTTLE.extract(cmdLine);
            this.minThreads = IGNORE;
            this.maxThreads = IGNORE;
        }
    }
//
//    private SettingsRate(int rate) {
//        this.auto = false;
//        this.isFixed = true;
//                this.threadCount = rate;
//                this.opsPerSecond = RATE_CLIENTS.dfltSupplier().get();
//                this.minThreads = IGNORE;
//                this.maxThreads = IGNORE;
//    }
//
//    public SettingsRate() {
//        this.auto = true;
//        this.isFixed = false;
//        this.threadCount = IGNORE;
//        this.opsPerSecond = RATE_CLIENTS.dfltSupplier().get();
//        this.minThreads = RATE_MIN_CLIENTS.dfltSupplier().get();
//        this.maxThreads = RATE_MAX_CLIENTS.dfltSupplier().get();
//    }

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
        return new Options()
               .addOptionGroup(AUTO_OR_THREADS)
               .addOption(RATE_MAX_CLIENTS.option())
               .addOption(RATE_MIN_CLIENTS.option())
               .addOption(RATE_CLIENTS.option());
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

//    public static SettingsRate get(CommandLine cmdLine, SettingsCommand command)
//    {
//        if (AUTO_OR_THREADS.getSelected() == null)
//        {
//            switch (command.type)
//            {
//                case WRITE:
//                case COUNTER_WRITE:
//                    if (command.count > 0)
//                    {
////
////                        ThreadOptions options = new ThreadOptions();
////                        options.accept("threads=200");
//                        return new SettingsRate(200);
//                    }
//            }
//            return new SettingsRate();
//        }
//        return new SettingsRate(cmdLine);
//    }

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

