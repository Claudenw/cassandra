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
}

