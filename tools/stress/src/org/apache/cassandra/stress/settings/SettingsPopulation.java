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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;

public class SettingsPopulation extends AbstractSettings implements Serializable
{

    /*
    this.distribution = dist.seed.get();
    or
            this.sequence = null;
            this.readlookback = null;
            this.wrap = false;
     */
    public static final StressOption<PartitionGenerator.Order> POPULATION_ORDER = new StressOption<>(()-> PartitionGenerator.Order.ARBITRARY,
                                                                                                     Option.builder("population-order").hasArg()
                                                                                                           .desc("Defines the (intra-)partition order. Valid values are: "+enumOptionString(PartitionGenerator.Order.ARBITRARY)+". (Default "+PartitionGenerator.Order.ARBITRARY+")")
                                                                                                           .type(PartitionGenerator.Order.class).build());

    public static final StressOption<long[]> POPULATION_SEQ = new StressOption<>(Option.builder("population-seq").hasArg()
                                                                                 .argName("start,end")
                                                                                       .desc("Generate all seeds in sequence")
                                                                                       .type(Long[].class)
                                                                                       .converter(DIST_CONVERTER)
                                                                                       .build());
    public static final StressOption<DistributionFactory> POPULATION_READ = new StressOption<>(Option.builder("population-read-lookback").hasArg()
                                                                                                     .desc(format("Select read seeds from the recently visited write seeds. Only applies if -%s is specified.", POPULATION_SEQ.key()))
                                                                                                     .type(DistributionFactory.class).build());
    public static final StressOption<String> POPULATION_NO_WRAP = new StressOption<>(Option.builder("population-no-wrap")
                                                                                           .desc(format("Terminate the stress test once all seeds in the range have been visited. Only applies if -%s is specified.", POPULATION_SEQ.key()))
                                                                                           .build());
    public static final StressOption<DistributionFactory> POPULATION_DIST = new StressOption<>(Option.builder("population-dist").hasArg()
                                                                                                     .desc("Seeds are selected from this distribution.")
                                                                                                     .type(DistributionFactory.class).build());


    private static final OptionGroup DIST_GROUP = new OptionGroup()
                                                  .addOption(POPULATION_DIST.option())
                                                  .addOption(POPULATION_SEQ.option());

    public final DistributionFactory distribution;
    public final DistributionFactory readlookback;
    public final PartitionGenerator.Order order;
    public final boolean wrap;
    public final long[] sequence;



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

//    private SettingsPopulation(PartitionGenerator.Order order, DistributionFactory distribution) {
//        this.order = order;
//            this.distribution = distribution;
//            this.sequence = null;
//            this.readlookback = null;
//            this.wrap = false;
//    }
//
//    private SettingsPopulation(PartitionGenerator.Order order, final long[] sequence,
//                               final DistributionFactory readlookback, boolean wrap) {
//        this.order = order;
//    this.distribution = null;
//    this.sequence = sequence;
//    this.readlookback = readlookback;
//    this.wrap = wrap;
//    }

//    private SettingsPopulation(CommandLine cmdLine, SettingsCommand settingsCommand)  {
//        this.order = POPULATION_ORDER.extract(cmdLine);
//        if (cmdLine.hasOption(POPULATION_DIST.option())) {
//            this.distribution =  POPULATION_DIST.extract(cmdLine);
//            this.sequence = null;
//            this.readlookback = null;
//            this.wrap = false;
//        } else
//        {
//            this.distribution = null;
//            this.sequence = POPULATION_SEQ.extract(cmdLine);
//            this.readlookback = POPULATION_READ.extract(cmdLine);
//            this.wrap = !cmdLine.hasOption(POPULATION_NO_WRAP.option());
//        }
//    }

    private class Builder {
        DistributionFactory distribution;
        DistributionFactory readlookback;

        long[] sequence;
        boolean wrap;

        public void setDistribution(DistributionFactory dist) {
            this.distribution = dist;
            this.readlookback = null;
            this.sequence = null;
            this.wrap = false;
        }

        public void setSequence(long[] sequence, DistributionFactory readlookback, boolean wrap) {
            this.distribution = null;
            this.sequence = sequence;
            this.readlookback = readlookback;
            this.wrap = wrap;
        }
    }

    public SettingsPopulation(CommandLine commandLine, SettingsCommand command) {
        Builder builder = new Builder();
        // set default size to number of commands requested, unless set to err convergence, then use 1M
        long defaultLimit = command.count <= 0 ? 1000000 : command.count;
        builder.setSequence(new long[] {1, defaultLimit}, POPULATION_READ.extract(commandLine), false);
        order = POPULATION_ORDER.extract(commandLine);
        if (DIST_GROUP.getSelected() == null)
        {
            if (!(command instanceof SettingsCommandUser && ((SettingsCommandUser)command).hasInsertOnly()))
            {
                switch (command.type)
                {
                    case WRITE:
                    case COUNTER_WRITE:
                        break;
                    default:
                        builder.setDistribution(OptionDistribution.get(String.format("gaussian(1..%s)", defaultLimit)));
                        break;
                }
            }
        }
        else if (commandLine.hasOption(POPULATION_DIST.option()))
        {
            builder.setDistribution(POPULATION_DIST.extract(commandLine));
        }
        else
        {
            builder.sequence = POPULATION_SEQ.extract(commandLine, builder.sequence);
            builder.wrap = !commandLine.hasOption(POPULATION_NO_WRAP.option());
        }
        this.readlookback = builder.readlookback;
        this.sequence = builder.sequence;
        this.distribution = builder.distribution;
        this.wrap = builder.wrap;
    }

    // Option Declarations
    public static Options getOptions()
    {
        return new Options()
        .addOptionGroup(DIST_GROUP)
        .addOption(POPULATION_READ.option())
        .addOption(POPULATION_NO_WRAP.option())
        .addOption(POPULATION_ORDER.option());
    }

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
}

