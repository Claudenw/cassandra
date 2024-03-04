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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

// Generic command settings - common to read/write/etc
public abstract class SettingsCommand extends AbstractSettings
{

    public static final StressOption<String> NO_WARMUP = new StressOption<>(new Option("no-warmup", "Do not warmup the process"));
    public static final StressOption<DurationSpec.IntSecondsBound> DURATION = new StressOption<>(Option.builder("duration").hasArg().type(DurationSpec.IntSecondsBound.class)
                                                                                                       .desc("Time to run. Not valid with -uncert-err or -n options.").build());
    public static final StressOption<Double> UNCERT_ERR = new StressOption<>(()->0.2d,
                                                                             rangeVerifier(0.0, Range.exclusive, 1.0, Range.exclusive),
                                                                             Option.builder("uncert-err")
                                                                                    .optionalArg(true)
                                                                                   .desc("Run until the standard error of the mean is below this fraction. Not valid with -duration or -n options. (Default 0.2)")
                                                                                   .type(Double.class).build());
    public static final StressOption<Integer> UNCERT_MIN = new StressOption<>(()->30,
                                                                              POSITIVE_VERIFIER,
                                                                              Option.builder("uncert-min").hasArg()
                                                                                    .desc(format("Run at least this many iterations before accepting uncertainty convergence. Only valid with %s. (Default 30)", UNCERT_ERR.key()))
                                                                                    .type(Integer.class).build());
    public static final StressOption<Integer> UNCERT_MAX = new StressOption<>(()->200,
                                                                              POSITIVE_VERIFIER,
                                                                              Option.builder("uncert-max").hasArg()
                                                                                    .desc(format("Run at least this many iterations before accepting uncertainty convergence. Only valid with %s. (Default 200)", UNCERT_ERR.key()))
                                                                                    .type(Integer.class).build());
    public static final StressOption<ConsistencyLevel> CONSISTENCY = new StressOption<>(()->ConsistencyLevel.LOCAL_ONE,
                                                                                        Option.builder("cl").hasArg()
                                                                                              .desc(format("Consistency level to use. Valid options are %s. (Default %s)", enumOptionString(ConsistencyLevel.LOCAL_ONE), ConsistencyLevel.LOCAL_ONE))
                                                                                              .type(ConsistencyLevel.class).converter(ConsistencyLevel::valueOf).build());

    public static final StressOption<TruncateWhen> TRUNCATE = new StressOption<>(()->TruncateWhen.NEVER,
                                                                                 Option.builder("truncate").hasArg()
                                                                                       .desc(format("When to truncate the table Valid values are %s. (Default %s)", enumOptionString(TruncateWhen.NEVER), TruncateWhen.NEVER))
                                                                                       .converter(s->TruncateWhen.valueOf(s.toUpperCase())).build());

    public static final StressOption<Long> COUNT = new StressOption<>(LONG_POSITIVE_VERIFIER, Option.builder("n").hasArg()
                                                                            .desc("Number of operations to perform. Number may be followd by 'm' or 'k' (e.g. 5m). Not valid with -duration or -uncert-err options.")
                                                                            .type(Long.class).converter(DISTRIBUTION_CONVERTER).required(true).build());


    private static final OptionGroup requiredGroup()
    {
        return new OptionGroup()
        {
            @Override
            public String toString()
            {
                StringBuilder buff = new StringBuilder();
                Iterator<Option> iter = this.getOptions().iterator();
                buff.append("One of the following options is required: ");

                while (iter.hasNext())
                {
                    Option option = (Option) iter.next();
                    if (option.getOpt() != null)
                    {
                        buff.append("-");
                        buff.append(option.getOpt());
                    }
                    else
                    {
                        buff.append("--");
                        buff.append(option.getLongOpt());
                    }

                    if (iter.hasNext())
                    {
                        buff.append(", ");
                    }
                }
                buff.append(".");
                return buff.toString();
            }
        }

               .addOption(COUNT.option())
               .addOption(DURATION.option())
               .addOption(UNCERT_ERR.option());
    }

    // predefined options
    public static final StressOption<Integer> COMMAND_KEYSIZE = new StressOption<>(()->10,
                                                                                   POSITIVE_VERIFIER,
                                                                                   Option.builder("command-keysize").hasArg().type(Integer.class).desc("Key size in bytes. (Default 10)").build());

    private static final String COMMAND_ADD_DEFAULT = "FIXED(1)";
    public static final StressOption<DistributionFactory> COMMAND_ADD = new StressOption<>(()->OptionDistribution.get(COMMAND_ADD_DEFAULT),
                                                                                           Option.builder("command-add").hasArg().type(DistributionFactory.class)
                                                                                                 .desc(format("Distribution of value of counter increments. (Default %s)", COMMAND_ADD_DEFAULT)).build());

    // predefined mixed and user shared options

    public static final StressOption<String> COMMAND_RATIO = new StressOption<>(Option.builder("command-ratio").hasArgs().valueSeparator().argName("ratio")
                                                                                      .desc("Specify the ratios for operations to perform. (see argument type notes below)").build());

    private static final String COMMAND_CLUSTERING_DEFAULT = "GAUSSIAN(1..10)";
    public static final StressOption<DistributionFactory> COMMAND_CLUSTERING = new StressOption<>(()->OptionDistribution.get(COMMAND_CLUSTERING_DEFAULT),
                                                                                                  Option.builder("command-clustering").hasArg().type(DistributionFactory.class)
                                                                                                        .desc(format("Distribution clustering runs of operations of the same kind. (Default %s)", COMMAND_CLUSTERING_DEFAULT)).build());

   // user command options

    public static final StressOption<String> COMMAND_PROFILE = new StressOption<>(Option.builder("command-profile").hasArgs().argName("file")
                                                                                      .desc("Specify the path to a yaml cql3 profile. Multiple files can be added.").build());

    public final Command type;
    public final long count;
    public final DurationSpec  duration;
    public final boolean noWarmup;
    public final TruncateWhen truncate;
    public final ConsistencyLevel consistencyLevel;
    public final double targetUncertainty;
    public final int minimumUncertaintyMeasurements;
    public final int maximumUncertaintyMeasurements;

    public abstract OpDistributionFactory getFactory(StressSettings settings);

    SettingsCommand(Command type, CommandLine commandLine) {
        try {
            this.type = type;
            this.consistencyLevel = CONSISTENCY.extract(commandLine);
            this.noWarmup = commandLine.hasOption(NO_WARMUP.option());
            this.truncate = TRUNCATE.extract(commandLine);

            if (commandLine.hasOption(COUNT.option())) {
                this.count = COUNT.extract(commandLine);
                this.duration = null;
                this.targetUncertainty = -1;
                this.minimumUncertaintyMeasurements = -1;
                this.maximumUncertaintyMeasurements = -1;
            } else if (commandLine.hasOption(DURATION.option())) {
                this.count = -1;
                this.duration = DURATION.extract(commandLine);
                this.targetUncertainty = -1;
                this.minimumUncertaintyMeasurements = -1;
                this.maximumUncertaintyMeasurements = -1;
            } else {
                this.count = -1;
                this.duration = null;
                this.targetUncertainty = UNCERT_ERR.extract(commandLine);
                this.minimumUncertaintyMeasurements = UNCERT_MIN.extract(commandLine);
                this.maximumUncertaintyMeasurements = UNCERT_MAX.extract(commandLine);
            }
        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }
//    public SettingsCommand(Command type, Options options, Count count, Duration duration, Uncertainty uncertainty)
//    {
//        this.type = type;
//        this.consistencyLevel = ConsistencyLevel.valueOf(options.consistencyLevel.value().toUpperCase());
//        this.noWarmup = options.noWarmup.setByUser();
//        this.truncate = TruncateWhen.valueOf(options.truncate.value().toUpperCase());
//
//        if (count != null)
//        {
//            this.count = OptionDistribution.parseLong(count.count.value());
//            this.duration = 0;
//            this.durationUnits = null;
//            this.targetUncertainty = -1;
//            this.minimumUncertaintyMeasurements = -1;
//            this.maximumUncertaintyMeasurements = -1;
//        }
//        else if (duration != null)
//        {
//            this.count = -1;
//            this.duration = Long.parseLong(duration.duration.value().substring(0, duration.duration.value().length() - 1));
//            switch (duration.duration.value().toLowerCase().charAt(duration.duration.value().length() - 1))
//            {
//                case 's':
//                    this.durationUnits = TimeUnit.SECONDS;
//                    break;
//                case 'm':
//                    this.durationUnits = TimeUnit.MINUTES;
//                    break;
//                case 'h':
//                    this.durationUnits = TimeUnit.HOURS;
//                    break;
//                case 'd':
//                    this.durationUnits = TimeUnit.DAYS;
//                    break;
//                default:
//                    throw new IllegalStateException();
//            }
//            this.targetUncertainty = -1;
//            this.minimumUncertaintyMeasurements = -1;
//            this.maximumUncertaintyMeasurements = -1;
//        }
//        else
//        {
//            this.count = -1;
//            this.duration = 0;
//            this.durationUnits = null;
//            this.targetUncertainty = Double.parseDouble(uncertainty.uncertainty.value());
//            this.minimumUncertaintyMeasurements = Integer.parseInt(uncertainty.minMeasurements.value());
//            this.maximumUncertaintyMeasurements = Integer.parseInt(uncertainty.maxMeasurements.value());
//        }
//    }

    // Option Declarations



    //final OptionSimple noWarmup = new OptionSimple("no-warmup", "", null, "Do not warmup the process", false);
    //        final OptionSimple truncate = new OptionSimple("truncate=", "never|once|always", "never", "Truncate the table: never, before performing any work, or before each iteration", false);
//        final OptionSimple consistencyLevel = new OptionSimple("cl=", "ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY|TWO|THREE|LOCAL_ONE|SERIAL|LOCAL_SERIAL", "LOCAL_ONE", "Consistency level to use", false);

    /**
     * Gets the shard options.  These are the options for all the settings command implementations.
     * @return the shared Options.
     */
    public static Options getOptions() {
        return new Options()
                .addOption(NO_WARMUP.option())
                .addOption(TRUNCATE.option())
                .addOption(CONSISTENCY.option())
                .addOptionGroup(requiredGroup())
                .addOption(UNCERT_MIN.option())
                .addOption(UNCERT_MAX.option())
                ;
    }

    /**
     * Returns all the possible options for command settings.
     * @return The complete set of options.
     */
    public static Options getAllOptions() {
        return  getOptions()
                         .addOption(COMMAND_KEYSIZE.option())
                         .addOption(COMMAND_ADD.option())
                         .addOption(COMMAND_RATIO.option())
                         .addOption(COMMAND_CLUSTERING.option())
                         .addOption(COMMAND_PROFILE.option());
    }
//    static abstract class Options extends GroupedOptions
//    {
//        final OptionSimple noWarmup = new OptionSimple("no-warmup", "", null, "Do not warmup the process", false);
//        final OptionSimple truncate = new OptionSimple("truncate=", "never|once|always", "never", "Truncate the table: never, before performing any work, or before each iteration", false);
//        final OptionSimple consistencyLevel = new OptionSimple("cl=", "ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY|TWO|THREE|LOCAL_ONE|SERIAL|LOCAL_SERIAL", "LOCAL_ONE", "Consistency level to use", false);
//    }
//
//    static class Count extends Options
//    {
//        final OptionSimple count = new OptionSimple("n=", "[0-9]+[bmk]?", null, "Number of operations to perform", true);
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(count, noWarmup, truncate, consistencyLevel);
//        }
//    }
//
//    static class Duration extends Options
//    {
//        final OptionSimple duration = new OptionSimple("duration=", "[0-9]+[smhd]", null, "Time to run in (in seconds, minutes, hours or days)", true);
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(duration, noWarmup, truncate, consistencyLevel);
//        }
//    }
//
//    static class Uncertainty extends Options
//    {
//        final OptionSimple uncertainty = new OptionSimple("err<", "0\\.[0-9]+", "0.02", "Run until the standard error of the mean is below this fraction", false);
//        final OptionSimple minMeasurements = new OptionSimple("n>", "[0-9]+", "30", "Run at least this many iterations before accepting uncertainty convergence", false);
//        final OptionSimple maxMeasurements = new OptionSimple("n<", "[0-9]+", "200", "Run at most this many iterations before accepting uncertainty convergence", false);
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(uncertainty, minMeasurements, maxMeasurements, noWarmup, truncate, consistencyLevel);
//        }
//    }

    public abstract void truncateTables(StressSettings settings);

    protected void truncateTables(StressSettings settings, String ks, String ... tables)
    {
        JavaDriverClient client = settings.getJavaDriverClient(false);
        assert settings.command.truncate != TruncateWhen.NEVER;
        for (String table : tables)
        {
            String cql = format("TRUNCATE %s.%s", ks, table);
            client.execute(cql, org.apache.cassandra.db.ConsistencyLevel.ONE);
        }
        System.out.format("Truncated %s.%s. Sleeping %ss for propagation.",
                                         ks, Arrays.toString(tables), settings.node.nodes.size());
        Uninterruptibles.sleepUninterruptibly(settings.node.nodes.size(), TimeUnit.SECONDS);
    }

    // CLI Utility Methods

    public void printSettings(ResultLogger out)
    {
        out.printf("  Type: %s%n", type.toString().toLowerCase());
        out.printf("  Count: %,d%n", count);
        if (duration != null)
        {
            out.printf("  Duration: %s%n", duration);
        }
        out.printf("  No Warmup: %s%n", noWarmup);
        out.printf("  Consistency Level: %s%n", consistencyLevel.toString());
        if (targetUncertainty != -1)
        {
            out.printf("  Target Uncertainty: %.3f%n", targetUncertainty);
            out.printf("  Minimum Uncertainty Measurements: %,d%n", minimumUncertaintyMeasurements);
            out.printf("  Maximum Uncertainty Measurements: %,d%n", maximumUncertaintyMeasurements);
        } else {
            out.printf("  Target Uncertainty: not applicable%n");
        }
    }


    static SettingsCommand get(Command cmd, CommandLine commandLine, Options options)
    {
        if (cmd.category == CommandCategory.HELP || commandLine.hasOption(SettingsMisc.MISC_HELP.option())) {
            return null;
        }


        OptionGroup required = options.getOptionGroup(COUNT.option());
        if (required.getSelected() == null)
            throw asRuntimeException( new MissingOptionException(required.toString()));

        switch (cmd.category)
        {
            case BASIC:
                return new SettingsCommandPreDefined(cmd, commandLine);
            case MIXED:
                return new SettingsCommandPreDefinedMixed(commandLine);
            case USER:
                return new SettingsCommandUser(commandLine);
            default:
                return null;
        }
    }

    public enum TruncateWhen
    {
        NEVER, ONCE, ALWAYS
    }
/*
    static void printHelp(Command type)
    {
        printHelp(type.toString().toLowerCase());
    }

    static void printHelp(String type)
    {
        GroupedOptions.printOptions(System.out, type.toLowerCase(), new Uncertainty(), new Count(), new Duration());
    }*/
}
