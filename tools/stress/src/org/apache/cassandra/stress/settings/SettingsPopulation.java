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
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;


import org.apache.commons.cli.*;

import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.commons.cli.converters.Converter;
import org.apache.commons.cli.converters.Verifier;


public class SettingsPopulation extends AbstractSettings implements Serializable
{

    public final DistributionFactory distribution;
    public final DistributionFactory readlookback;
    public final PartitionGenerator.Order order;
    public final boolean wrap;
    public final long[] sequence;

    private static final String ORDER="population-order";
    private static final String DIST="population-dist";
    private static final String READ="population-read-lookback";
    private static final String NO_WRAP="population-no-wrap";
    private static final String SEQ="population-seq";


    private static final OptionGroup DIST_GROUP;
    private static final Options OPTIONS;

    static {
        OPTIONS = new Options();

        OPTIONS.addOption(org.apache.commons.cli.Option.builder(ORDER).hasArg().desc( "Defines the (intra-)partition order; if not specified, will be consistent but arbitrary order.  Valid values are: "+
                        String.join(", ", (List<String>)Arrays.stream(PartitionGenerator.Order.values()).map(Enum::name).collect(Collectors.toList())))
                .type(PartitionGenerator.Order.class).build());
        DIST_GROUP = new OptionGroup();
        DIST_GROUP.addOption(org.apache.commons.cli.Option.builder(DIST).hasArg().desc("Seeds are selected from this distribution").type(DistributionFactory.class).build());
        DIST_GROUP.addOption(org.apache.commons.cli.Option.builder(SEQ).hasArg().desc("Generate all seeds in sequence").type(long[].class).converter(DIST_CONVERTER).build());
        OPTIONS.addOptionGroup(DIST_GROUP);
        OPTIONS.addOption(org.apache.commons.cli.Option.builder(READ).desc("Select read seeds from the recently visited write seeds.  Only applies if "+SEQ+" is specified")
                .hasArg().type(DistributionFactory.class).build());
        OPTIONS.addOption(new org.apache.commons.cli.Option(NO_WRAP, "Terminate the stress test once all seeds in the range have been visited.  Only applies if "+SEQ+" is specified"));
    }

//    private SettingsPopulation(GenerateOptions options, DistributionOptions dist, SequentialOptions pop)
//    {
//        this.order = !options.contents.setByUser() ? PartitionGenerator.Order.ARBITRARY : PartitionGenerator.Order.valueOf(options.contents.value().toUpperCase());
//        if (dist != null)
//        {
//            this.distribution = dist.seed.get();
//            this.sequence = null;
//            this.readlookback = null;
//            this.wrap = false;
//        }
//        else
//        {
//            this.distribution = null;
//            String[] bounds = pop.populate.value().split("\\.\\.+");
//            this.sequence = new long[] { OptionDistribution.parseLong(bounds[0]), OptionDistribution.parseLong(bounds[1]) };
//            this.readlookback = pop.lookback.get();
//            this.wrap = !pop.nowrap.setByUser();
//        }
//    }
//
//    public SettingsPopulation(DistributionOptions options)
//    {
//        this(options, options, null);
//    }
//
//    public SettingsPopulation(SequentialOptions options)
//    {
//        this(options, null, options);
//    }

    private SettingsPopulation(PartitionGenerator.Order order, DistributionFactory distribution) {
        this.order = order;
            this.distribution = distribution;
            this.sequence = null;
            this.readlookback = null;
            this.wrap = false;
    }

    private SettingsPopulation(PartitionGenerator.Order order, final long[] sequence,
                               final DistributionFactory readlookback, boolean wrap) {
        this.order = order;
    this.distribution = null;
    this.sequence = sequence;
    this.readlookback = readlookback;
    this.wrap = wrap;
    }

//    private SettingsPopulation(CommandLine cmdLine) throws ParseException {
//        this.order = cmdLine.getParsedOptionValue(ORDER);
//        if (cmdLine.hasOption(DIST)) {
//            this.distribution =  cmdLine.getParsedOptionValue(DIST);
//            this.sequence = null;
//            this.readlookback = null;
//            this.wrap = false;
//        } else
//        {
//            this.distribution = null;
//            this.sequence = cmdLine.getParsedOptionValue(SEQ);
//            this.readlookback = cmdLine.getParsedOptionValue(READ);
//            this.wrap = !cmdLine.hasOption(NO_WRAP);
//        }
//    }


    public static SettingsPopulation get(CommandLine cmdLine, SettingsCommand command) {
        // set default size to number of commands requested, unless set to err convergence, then use 1M
        long defaultLimit = command.count <= 0 ? 1000000 : command.count;
        try {
        PartitionGenerator.Order order = cmdLine.getParsedOptionValue(ORDER);

        if (DIST_GROUP.getSelected() == null)
        {
            if (command instanceof SettingsCommandUser && ((SettingsCommandUser)command).hasInsertOnly())
            {
                return new SettingsPopulation(order, new long[] {1, defaultLimit},
                                              cmdLine.hasOption(READ) ? OptionDistribution.get(cmdLine.getOptionValue(READ)) : null,
                                              !cmdLine.hasOption(NO_WRAP));
            }

            // return defaults:
            switch(command.type)
            {
                case WRITE:
                case COUNTER_WRITE:
                    return new SettingsPopulation(order, new long[] {1, defaultLimit}, cmdLine.getParsedOptionValue(READ), !cmdLine.hasOption(NO_WRAP));
                default:
                    return new SettingsPopulation(order, OptionDistribution.get( String.format("gaussian(1..%s)", defaultLimit)));
            }
        }
        if (cmdLine.hasOption(DIST)) {
            return new SettingsPopulation(order, OptionDistribution.get( cmdLine.getOptionValue(DIST)));
        } else {
            return new SettingsPopulation(order, cmdLine.getParsedOptionValue(SEQ, DIST_CONVERTER.apply("1.." + defaultLimit)),
                    cmdLine.getParsedOptionValue(READ), !cmdLine.hasOption(NO_WRAP));
        }
        } catch (Exception ex) {
            throw asRuntimeException(ex);
        }
    }


    public static Options getOptions()
    {
        return OPTIONS;
    }

    // Option Declarations

//    private static class GenerateOptions extends GroupedOptions
//    {
//        final OptionSimple contents = new OptionSimple("contents=", "(sorted|shuffled)", null, "SORTED or SHUFFLED (intra-)partition order; if not specified, will be consistent but arbitrary order", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(contents);
//        }
//    }
//
//    private static final class DistributionOptions extends GenerateOptions
//    {
//        final OptionDistribution seed;
//
//        public DistributionOptions(String defaultLimit)
//        {
//            seed = new OptionDistribution("dist=", "gaussian(1.." + defaultLimit + ")", "Seeds are selected from this distribution");
//        }
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return ImmutableList.<Option>builder().add(seed).addAll(super.options()).build();
//        }
//    }
//
//    private static final class SequentialOptions extends GenerateOptions
//    {
//        final OptionSimple populate;
//        final OptionDistribution lookback = new OptionDistribution("read-lookback=", null, "Select read seeds from the recently visited write seeds", false);
//        final OptionSimple nowrap = new OptionSimple("no-wrap", "", null, "Terminate the stress test once all seeds in the range have been visited", false);
//
//        public SequentialOptions(String defaultLimit)
//        {
//            populate = new OptionSimple("seq=", "[0-9]+[MBK]?\\.\\.+[0-9]+[MBK]?",
//                    "1.." + defaultLimit,
//                    "Generate all seeds in sequence", true);
//        }
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return ImmutableList.<Option>builder().add(populate, nowrap, lookback).addAll(super.options()).build();
//        }
//    }

    // CLI Utility Methods

    public void printSettings(ResultLogger out)
    {
        if (distribution != null)
        {
            out.println("  Distribution: " +distribution.getConfigAsString());
        }

        if (sequence != null)
        {
            out.printf("  Sequence: %d..%d%n", sequence[0], sequence[1]);
        }
        if (readlookback != null)
        {
            out.println("  Read Look Back: " + readlookback.getConfigAsString());
        }

        out.printf("  Order: %s%n", order);
        out.printf("  Wrap: %b%n", wrap);
    }

//    public static SettingsPopulation get(Map<String, String[]> clArgs, SettingsCommand command)
//    {
//        // set default size to number of commands requested, unless set to err convergence, then use 1M
//        String defaultLimit = command.count <= 0 ? "1000000" : Long.toString(command.count);
//
//        String[] params = clArgs.remove("-pop");
//        if (params == null)
//        {
//            if (command instanceof SettingsCommandUser && ((SettingsCommandUser)command).hasInsertOnly())
//            {
//                return new SettingsPopulation(new SequentialOptions(defaultLimit));
//            }
//
//            // return defaults:
//            switch(command.type)
//            {
//                case WRITE:
//                case COUNTER_WRITE:
//                    return new SettingsPopulation(new SequentialOptions(defaultLimit));
//                default:
//                    return new SettingsPopulation(new DistributionOptions(defaultLimit));
//            }
//        }
//        GroupedOptions options = GroupedOptions.select(params, new SequentialOptions(defaultLimit), new DistributionOptions(defaultLimit));
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -pop options provided, see output for valid options");
//            System.exit(1);
//        }
//        return options instanceof SequentialOptions ?
//                new SettingsPopulation((SequentialOptions) options) :
//                new SettingsPopulation((DistributionOptions) options);
//    }
//
//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-pop", new SequentialOptions("N"), new DistributionOptions("N"));
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

