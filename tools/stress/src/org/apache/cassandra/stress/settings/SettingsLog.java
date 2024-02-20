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
    public static final StressOption<DurationSpec.IntSecondsBound> LOG_INTERVAL = new StressOption<>(Option.builder("log-interval").desc("Log progress every <value> seconds or milliseconds.  Should have a pattern like 2s or 3000ms")
                                                                                     .required().hasArg().type(DurationSpec.IntSecondsBound.class).build());
    public static final StressOption<Level> LOG_LEVEL = new StressOption<>(Option.builder("log-level").desc("The Level to log at. Valid options are: "+AbstractSettings.enumOptionString(Level.VERBOSE))
                                                                                 .type(Level.class).converter(s -> Level.valueOf(s.toUpperCase()))
                                                                           .hasArg().argName("level").required().build());

    public static enum Level
    {
        MINIMAL, NORMAL, VERBOSE
    }

    public final boolean noSummary;
    public final boolean noSettings;
    public final File file;
    public final File hdrFile;
    public final int intervalMillis;
    public final Level level;

    public SettingsLog(CommandLine commandLine)
    {

        noSummary = commandLine.hasOption(LOG_NO_SUMMARY.option());
        noSettings = commandLine.hasOption(LOG_NO_SETTINGS.option());
        file = LOG_FILE.extract(commandLine);
        hdrFile = LOG_HEADER_FILE.extract(commandLine);

        DurationSpec.IntSecondsBound interval = LOG_INTERVAL.extract(commandLine);
        intervalMillis = interval.toMilliseconds();
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

//    public static final class Options extends GroupedOptions
//    {
//        final OptionSimple noSummmary = new OptionSimple("no-summary", "", null, "Disable printing of aggregate statistics at the end of a test", false);
//        final OptionSimple noSettings = new OptionSimple("no-settings", "", null, "Disable printing of settings values at start of test", false);
//        final OptionSimple outputFile = new OptionSimple("file=", ".*", null, "Log to a file", false);
//        final OptionSimple hdrOutputFile = new OptionSimple("hdrfile=", ".*", null, "Log to a file", false);
//        final OptionSimple interval = new OptionSimple("interval=", "[0-9]+(ms|s|)", "1s", "Log progress every <value> seconds or milliseconds", false);
//        final OptionSimple level = new OptionSimple("level=", "(minimal|normal|verbose)", "normal", "Logging level (minimal, normal or verbose)", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(level, noSummmary, outputFile, hdrOutputFile, interval, noSettings);
//        }
//    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  No Summary: %b%n", noSummary);
        out.printf("  No Settings: %b%n", noSettings);
        out.printf("  File: %s%n", file);
        out.printf("  Interval Millis: %d%n", intervalMillis);
        out.printf("  Level: %s%n", level);
    }

//
//    public static SettingsLog get(Map<String, String[]> clArgs)
//    {
//        String[] params = clArgs.remove("-log");
//        if (params == null)
//            return new SettingsLog(new Options());
//
//        GroupedOptions options = GroupedOptions.select(params, new Options());
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -log options provided, see output for valid options");
//            System.exit(1);
//        }
//        return new SettingsLog((Options) options);
//    }
//
//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-log", new Options());
//    }
//
//    public static Runnable helpPrinter()
//    {
//        return new Runnable()
//        {
//            @Override
//            public void run()
//            {
//                printHelp();
//            }
//        };
//    }
}
