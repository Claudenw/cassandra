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



import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.stress.util.MultiResultLogger;
import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsLog extends AbstractSettings
{
    public static final StressOption<String> LOG_NO_SUMMARY = new StressOption<>(new Option("log-no-summary", "Disable printing of aggregate statistics at the end of a test."));
    public static final StressOption<String> LOG_NO_SETTINGS = new StressOption<>(new Option("log-no-settings", "Disable printing of settings values at start of test."));
    public static final StressOption<File> LOG_FILE = new StressOption<>(Option.builder("log-file").desc("Log to the specified file.").hasArg().type(File.class).build());
    public static final StressOption<File> LOG_HEADER_FILE = new StressOption<>(Option.builder("log-header-file").desc("Log headers to the specified file.").hasArg().type(File.class).build());
    public static final StressOption<DurationSpec.IntSecondsBound> LOG_INTERVAL = new StressOption<>(() -> new DurationSpec.IntSecondsBound("1s"),
                                                                                                     Option.builder("log-interval").desc("Log progress every <value> seconds or milliseconds.  Should have a pattern like 2s or 3000ms")
                                                                                     .hasArg().type(DurationSpec.IntSecondsBound.class).build());
    public static final StressOption<Level> LOG_LEVEL = new StressOption<>(() -> Level.NORMAL,
                                                                           Option.builder("log-level").desc("Logging level. Valid options are: "+AbstractSettings.enumOptionString(Level.VERBOSE)+". (Default = MORMAL)")
                                                                                 .type(Level.class).converter(s -> Level.valueOf(s.toUpperCase()))
                                                                                 .hasArg().argName("level").build());

    public static enum Level
    {
        MINIMAL, NORMAL, VERBOSE
    }

    public final boolean noSummary;
    public final boolean noSettings;
    public final File file;
    public final File hdrFile;
    public final DurationSpec interval;
    public final Level level;

    public SettingsLog(CommandLine commandLine)
    {
        noSummary = commandLine.hasOption(LOG_NO_SUMMARY.option());
        noSettings = commandLine.hasOption(LOG_NO_SETTINGS.option());
        file = LOG_FILE.extract(commandLine);
        hdrFile = LOG_HEADER_FILE.extract(commandLine);
        interval = LOG_INTERVAL.extract(commandLine);
        level = LOG_LEVEL.extract(commandLine);
    }

    public MultiResultLogger getOutput() throws FileNotFoundException
    {
        // Always print to stdout regardless of whether we're graphing or not
        MultiResultLogger stream = new MultiResultLogger(new PrintStream(System.out));

        if (file != null)
            stream.addStream(new PrintStream(file));

        return stream;
    }

    // Option Declarations

    public static Options getOptions() {
        return new org.apache.commons.cli.Options()
               .addOption(LOG_INTERVAL.option())
               .addOption(LOG_NO_SUMMARY.option())
               .addOption(LOG_LEVEL.option())
               .addOption(LOG_NO_SETTINGS.option())
               .addOption(LOG_HEADER_FILE.option())
               .addOption(LOG_FILE.option())
        ;
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  No Summary: %b%n", noSummary);
        out.printf("  No Settings: %b%n", noSettings);
        out.printf("  File: %s%n", file);
        out.printf("  Interval: %s%n", interval);
        out.printf("  Level: %s%n", level);
    }
}
