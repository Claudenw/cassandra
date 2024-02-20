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


import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.cassandra.stress.generate.Distribution;
import org.apache.commons.cli.Options;

class SettingsMisc extends AbstractSettings implements Serializable
{
    public static final StressOption<Command> MISC_HELP = new StressOption<>(Option.builder("h").longOpt("help").hasArg().desc("Help for entire system or for each set of commands")
            .converter(s -> Command.valueOf(s.toUpperCase())).build());
    public static final StressOption<DistributionFactory> MISC_DISTRIBUTION = new StressOption<>(Option.builder("print-dist").hasArg().desc("A mathematical distribution").converter(DIST_CONVERTER).build());
    public static final StressOption<String> MISC_VERSION = new StressOption<>(Option.builder("version").desc("Pirnts the version").build());


    public static Options getOptions()
    {
        return new Options()
                .addOption(MISC_HELP.option())
                .addOption(MISC_DISTRIBUTION.option())
                .addOption(MISC_VERSION.option());
    }

    static boolean maybeDoSpecial(CommandLine commandLine)
    {
        return maybePrintHelp(commandLine) || maybePrintDistribution(commandLine) ||
                maybePrintVersion(commandLine);
    }

//    private static final class PrintDistribution extends GroupedOptions
//    {
//        final OptionDistribution dist = new OptionDistribution("dist=", null, "A mathematical distribution");
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(dist);
//        }
//    }


    private static boolean maybePrintDistribution(CommandLine commandLine)
    {
        if (commandLine.hasOption(MISC_DISTRIBUTION.option()))
        {
            printHelp();
            System.out.println("Invalid print options provided, see output for valid options");
            System.exit(1);
        }
        printDistribution(MISC_DISTRIBUTION.extract(commandLine).get());
        return true;
    }

    private static void printDistribution(Distribution dist)
    {
        PrintStream out = System.out;
        out.println("% of samples    Range       % of total");
        String format = "%-16.1f%-12d%12.1f";
        double rangemax = dist.inverseCumProb(1d) / 100d;
        for (double d : new double[]{ 0.1d, 0.2d, 0.3d, 0.4d, 0.5d, 0.6d, 0.7d, 0.8d, 0.9d, 0.95d, 0.99d, 1d })
        {
            double sampleperc = d * 100;
            long max = dist.inverseCumProb(d);
            double rangeperc = max / rangemax;
            out.println(String.format(format, sampleperc, max, rangeperc));
        }
    }

    private static boolean maybePrintHelp(CommandLine commandLine)
    {
        if (commandLine.hasOption(MISC_HELP.option()))
        {
            Command command = MISC_HELP.extract(commandLine);
            if (command != null) {
                command.printHelp();
            }
            printHelp(commandLine);
        }
        return false;
    }

    private static boolean maybePrintVersion(CommandLine commandLine)
    {
        if (commandLine.hasOption(MISC_VERSION.option()))
        {
            try   {
                URL url = Resources.getResource("org/apache/cassandra/config/version.properties");
                System.out.println(parseVersionFile(Resources.toString(url, Charsets.UTF_8)));
            }
            catch (IOException e)
            {
                e.printStackTrace(System.err);
            }
            return true;
        }
        return false;
    }

    static String parseVersionFile(String versionFileContents)
    {
        Matcher matcher = Pattern.compile(".*?CassandraVersion=(.*?)$").matcher(versionFileContents);
        if (matcher.find())
        {
            return "Version: " + matcher.group(1);
        }
        else
        {
            return "Unable to find version information";
        }
    }

    protected static String createPadding(int len) {
        char[] padding = new char[len];
        Arrays.fill(padding, ' ');
        return new String(padding);
    }

    public static void printHelp()
    {
        try (PrintWriter pw =  new PrintWriter(System.out))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(130);
            formatter.setLongOptPrefix("");

            pw.println("Usage:      cassandra-stress <command> [options]");
            pw.println("Help usage: cassandra-stress help <command>");
            pw.println();
            pw.println("---Commands---");

            Options options = new Options();
            Arrays.stream(Command.values()).map( c -> Option.builder(null).longOpt(c.name()).desc(c.description).build()).forEach(options::addOption);
//            for (Command cmd : Command.values())
//            {
//                formatter.printWrapped(String.format("%-20s : %s", cmd.toString().toLowerCase(), cmd.description),);
//            }
            formatter.printOptions(pw, formatter.getWidth(), options, formatter.getLeftPadding(), formatter.getDescPadding());


            pw.println("\n---Options---");
            formatter.printOptions(pw, formatter.getWidth(), StressSettings.getOptions(), formatter.getLeftPadding(), formatter.getDescPadding());

            pw.println("\n---Argument Types---");
            String argumentPadding = createPadding(formatter.getLeftPadding()+5);
            for (StressArgument sa : AbstractSettings.argumentMap.values())
            {
                if (!sa.notes.isEmpty() || !sa.options.getOptions().isEmpty())
                {
                    pw.format("\n<%s>\n", sa.name);
                    if (!sa.options.getOptions().isEmpty())
                        formatter.printOptions(pw, formatter.getWidth(), sa.options, formatter.getLeftPadding() + 5, formatter.getDescPadding() + 5);
                    if (!sa.notes.isEmpty())
                        sa.notes.stream().map(s -> argumentPadding + s).forEach(s -> formatter.printWrapped(pw, formatter.getWidth(), formatter.getLeftPadding() + 10, s.toString()));
                }
            }

        }
    }

    public static void printHelp(CommandLine commandLine)
    {
        HelpFormatter formatter = new HelpFormatter();
        Options options = null;
        if (commandLine.getOptions().length > 0)
        {
            options = new Options();
            Arrays.stream(commandLine.getOptions()).forEach(options::addOption);
        } else {
            options = StressSettings.getOptions();
        }
        formatter.printHelp("help", options);
    }

//    static Runnable helpHelpPrinter()
//    {
//        return () -> {
//            System.out.println("Usage: ./bin/cassandra-stress help <command|option>");
//            System.out.println("Commands:");
//            for (Command cmd : Command.values())
//                System.out.println("    " + cmd.names.toString().replaceAll("\\[|\\]", ""));
//            System.out.println("Options:");
//            for (CliOption op : CliOption.values())
//                System.out.println("    -" + op.toString().toLowerCase() + (op.extraName != null ? ", " + op.extraName : ""));
//        };
//    }
//
//    static Runnable printHelpPrinter()
//    {
//        return () -> GroupedOptions.printOptions(System.out, "print", new GroupedOptions()
//        {
//            @Override
//            public List<? extends Option> options()
//            {
//                return Arrays.asList(new OptionDistribution("dist=", null, "A mathematical distribution"));
//            }
//        });
//    }
}
