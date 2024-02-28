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

public class SettingsMisc extends AbstractSettings implements Serializable
{
    public static final StressOption<String> MISC_HELP = new StressOption<>(Option.builder("?").longOpt("help")
                                                                                   .desc("Prints help for the current command.").build());
    public static final StressOption<DistributionFactory> MISC_DISTRIBUTION = new StressOption<>(Option.builder("print-dist").hasArg().desc("A mathematical distribution").type(DistributionFactory.class).build());
    public static final StressOption<String> MISC_VERSION = new StressOption<>(Option.builder("version").desc("Prints the version").build());


    private final Command helpCommand;
    private final DistributionFactory factory;

    private final boolean versionReq;

    public static Options getOptions()
    {
        return new Options()
                .addOption(MISC_HELP.option())
                .addOption(MISC_DISTRIBUTION.option())
                .addOption(MISC_VERSION.option());
    }

    public SettingsMisc(CommandLine commandLine, Command cmd)
    {
        helpCommand = cmd == Command.HELP ? Command.HELP :
                      (commandLine.hasOption(MISC_HELP.option()) ? cmd : null);

        factory = MISC_DISTRIBUTION.extract(commandLine);

        versionReq = commandLine.hasOption(MISC_VERSION.option()) || cmd == Command.VERSION;

    }

    public boolean maybeDoSpecial()
    {
        if (helpCommand != null) {
            printHelp(helpCommand);
        }

        if (factory != null) {
            printDistribution(factory.get());
        }

        if (versionReq)
            printVersion();

        return versionReq || helpCommand != null || factory != null;
    }

    private void printDistribution(Distribution dist)
    {
        PrintStream out = System.out;
        out.println("Distribution report for: "+factory.getConfigAsString());
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

    private static void printVersion()
    {
        try   {
            URL url = Resources.getResource("org/apache/cassandra/config/version.properties");
            System.out.println(parseVersionFile(Resources.toString(url, Charsets.UTF_8)));
        }
        catch (IOException e)
        {
            e.printStackTrace(System.err);
        }
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

    public static void printHelp(Command command)
    {
        try (PrintWriter pw =  new PrintWriter(System.out))
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(130);
            formatter.setLongOptPrefix("");

            pw.format("Usage:      cassandra-stress %s [options]\n", command == null ? "<command>" : command);
//            pw.println("Help usage: cassandra-stress help <command>");
            pw.println();
            pw.println("---Commands---");

            Options options = new Options();
            if (command == Command.HELP)
            {
                Arrays.stream(Command.values()).map(c -> Option.builder(null).longOpt(c.name()).desc(c.description).build()).forEach(options::addOption);
            } else {
                options.addOption(Option.builder(null).longOpt(command.name()).desc(command.description).build());
            }
            formatter.printOptions(pw, formatter.getWidth(), options, formatter.getLeftPadding(), formatter.getDescPadding());


            pw.println("\n---Options---");
            formatter.printOptions(pw, formatter.getWidth(), getCommandOptions(command), formatter.getLeftPadding(), formatter.getDescPadding());

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

    private static Options getCommandOptions(Command command) {
        switch (command)
        {
            case PRINT: // print the distribution
            case VERSION:
                return new Options();
            case HELP:
                return StressSettings.getOptions();
            default:
                Options result = StressSettings.getSharedOptions();
                switch (command.category)
                {
                    case USER:
                        result.addOptions(SettingsCommandUser.getOptions());
                        break;
                    case BASIC:
                        result.addOptions(SettingsCommandPreDefined.getOptions());
                        break;
                    case MIXED:
                        result.addOptions(SettingsCommandPreDefinedMixed.getOptions());
                        break;
                }
                return result;
        }
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
