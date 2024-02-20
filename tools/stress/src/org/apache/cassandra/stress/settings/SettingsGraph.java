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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class SettingsGraph extends AbstractSettings
{
    public final String file;
    public final String revision;
    public final String title;
    public final String operation;
    public final File temporaryLogFile;

    public static final StressOption<String> GRAPH_FILE = new StressOption<>(org.apache.commons.cli.Option.builder("graph-file").required().hasArg().argName("file").desc("HTML file to create or append to.").build());
    public static final StressOption<String> GRAPH_REVISION = new StressOption<>(new org.apache.commons.cli.Option("graph-revision", true, "Unique name to assign to the current configuration being stressed."));
    public static final StressOption<String> GRAPH_TITLE = new StressOption<>(new org.apache.commons.cli.Option("graph-title", true, "Title for chart. (Default: current date)"));
    public static final StressOption<String> GRAPH_NAME = new StressOption<>(new Option("graph-name", true, "Alternative name for current operation (Default: stress op name)"));

    public SettingsGraph(CommandLine commandLine, SettingsCommand stressCommand)
    {
        file = commandLine.getOptionValue(GRAPH_FILE.option());
        revision = commandLine.getOptionValue(GRAPH_REVISION.option());
        title = commandLine.getOptionValue(GRAPH_TITLE.option(), ()-> "cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm:ss").format(new Date()));
        operation = commandLine.getOptionValue(GRAPH_NAME.option(), stressCommand.type::name);

        if (inGraphMode())
        {
            temporaryLogFile = FileUtils.createTempFile("cassandra-stress", ".log").toJavaIOFile();
        }
        else
        {
            temporaryLogFile = null;
        }
    }

    public boolean inGraphMode()
    {
        return this.file == null ? false : true;
    }

    // Option Declarations
//    private static final class GraphOptions extends GroupedOptions
//    {
//        final OptionSimple file = new OptionSimple("file=", ".*", null, "HTML file to create or append to", true);
//        final OptionSimple revision = new OptionSimple("revision=", ".*", "unknown", "Unique name to assign to the current configuration being stressed", false);
//        final OptionSimple title = new OptionSimple("title=", ".*", null, "Title for chart (current date by default)", false);
//        final OptionSimple operation = new OptionSimple("op=", ".*", null, "Alternative name for current operation (stress op name used by default)", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(file, revision, title, operation);
//        }
//    }

    // CLI Utility Methods

    public static Options getOptions() {
        return new Options()
                .addOption(GRAPH_FILE.option())
                .addOption(GRAPH_REVISION.option())
                .addOption(GRAPH_TITLE.option())
                .addOption(GRAPH_NAME.option());
    }
    public void printSettings(ResultLogger out)
    {
        out.println("  File: " + file);
        out.println("  Revision: " + revision);
        out.println("  Title: " + title);
        out.println("  Operation: " + operation);
    }


//    public static SettingsGraph get(Map<String, String[]> clArgs, SettingsCommand stressCommand)
//    {
//        String[] params = clArgs.remove("-graph");
//        if (params == null)
//        {
//            return new SettingsGraph(new GraphOptions(), stressCommand);
//        }
//        GraphOptions options = GroupedOptions.select(params, new GraphOptions());
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -graph options provided, see output for valid options");
//            System.exit(1);
//        }
//        return new SettingsGraph(options, stressCommand);
//    }
//
//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-graph", new GraphOptions());
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

