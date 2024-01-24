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
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.StressProfile;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.generate.TokenRangeIterator;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.operations.SampledOpDistributionFactory;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Converter;
import org.apache.commons.cli.Options;

// Settings unique to the mixed command type
public class SettingsCommandUser extends SettingsCommand
{

    // Ratios for selecting commands - index for each Command, NaN indicates the command is not requested
    private final Map<String, Double> ratios;
    private final DistributionFactory clustering;
    public final Map<String,  StressProfile> profiles;
    private final CommandLine commandLine;
    private String default_profile_name;
    private static final Pattern EXTRACT_SPEC_CMD = Pattern.compile("(.+)\\.(.+)");


    public SettingsCommandUser(CommandLine commandLine)
    {
        super(Command.USER, commandLine);

        this.commandLine = commandLine;
        try {
            clustering = commandLine.getParsedOptionValue(StressOption.COMMAND_CLUSTERING.option(), StressOption.COMMAND_CLUSTERING.dfltSupplier());
        /*
           .addOption(StressOption.COMMAND_CLUSTERING.option())
                .addOption(StressOption.COMMAND_PROFILE.option())
                .addOption(StressOption.COMMAND_RATIO.option());
         */
            //ratios = commandLine.get

            if (commandLine.hasOption(StressOption.COMMAND_RATIO.option())) {
                ratios = ratios(s->s, commandLine.getOptionValues(StressOption.COMMAND_RATIO.option()));
                if (ratios.size() == 0)
                    throw new IllegalArgumentException("Must specify at least one command with a non-zero ratio");
            } else {
                ratios = Collections.emptyMap();
            }

            default_profile_name = null;


            profiles = new LinkedHashMap<>();

            for (String yamlPath : commandLine.getOptionValues(StressOption.COMMAND_PROFILE.option())) {

                File yamlFile = new File(yamlPath);
                URI yamlURI;
                if (yamlFile.exists()) {
                    yamlURI = yamlFile.toURI();
                } else {
                    yamlURI = URI.create(yamlPath);
                    String uriScheme = yamlURI.getScheme();
                    if (uriScheme == null || "file".equals(uriScheme)) {
                        throw new IllegalArgumentException("File '" + yamlURI.getPath() + "' doesn't exist!");
                    }
                }
                StressProfile profile = StressProfile.load(yamlURI);
                String specName = profile.specName;
                if (default_profile_name == null) {
                    default_profile_name = specName;
                } //first file is default
                if (profiles.containsKey(specName)) {
                    throw new IllegalArgumentException("Must only specify a singe YAML file per table (including keyspace qualifier).");
                }
                profiles.put(specName, profile);
            }

        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }

    public boolean hasInsertOnly()
    {
        return ratios.size() == 1 && ratios.containsKey("insert");
    }

    public OpDistributionFactory getFactory(final StressSettings settings)
    {
        final SeedManager seeds = new SeedManager(settings);

        final Map<String, TokenRangeIterator> tokenRangeIterators = new LinkedHashMap<>();
        profiles.forEach((k,v)->tokenRangeIterators.put(k, (v.tokenRangeQueries.isEmpty()
                                                            ? null
                                                            : new TokenRangeIterator(settings,
                                                                                     v.maybeLoadTokenRanges(settings)))));

        return new SampledOpDistributionFactory<String>(ratios, clustering)
        {
            protected List<? extends Operation> get(Timer timer, String key, boolean isWarmup)
            {
                Matcher m = EXTRACT_SPEC_CMD.matcher(key);
                final String profile_name;
                final String sub_key;
                if (m.matches())
                {
                    profile_name = m.group(1);
                    sub_key = m.group(2);
                }
                else
                {
                    profile_name = default_profile_name;
                    sub_key = key;
                }

                if (!profiles.containsKey(profile_name))
                {
                    throw new IllegalArgumentException(String.format("Op name %s contains an invalid profile specname: %s", key, profile_name));
                }
                StressProfile profile = profiles.get(profile_name);
                TokenRangeIterator tokenRangeIterator = tokenRangeIterators.get(profile_name);
                PartitionGenerator generator = profile.newGenerator(settings);
                if (sub_key.equalsIgnoreCase("insert"))
                    return Collections.singletonList(profile.getInsert(timer, generator, seeds, settings));
                if (sub_key.equalsIgnoreCase("validate"))
                    return profile.getValidate(timer, generator, seeds, settings);

                if (profile.tokenRangeQueries.containsKey(sub_key))
                    return Collections.singletonList(profile.getBulkReadQueries(sub_key, timer, settings, tokenRangeIterator, isWarmup));

                return Collections.singletonList(profile.getQuery(sub_key, timer, generator, seeds, settings, isWarmup));
            }
        };
    }

    public void truncateTables(StressSettings settings)
    {
        profiles.forEach((k,v)-> v.truncateTable(settings));
    }

//    static final class Options extends GroupedOptions
//    {
//        final SettingsCommand.Options parent;
//        protected Options(SettingsCommand.Options parent)
//        {
//            this.parent = parent;
//        }
//        final OptionDistribution clustering = new OptionDistribution("clustering=", "gaussian(1..10)", "Distribution clustering runs of operations of the same kind");
//        final OptionSimple profile = new OptionSimple("profile=", ".*", null, "Specify the path to a yaml cql3 profile. Multiple comma separated files can be added.", true);
//        final OptionAnyProbabilities ops = new OptionAnyProbabilities("ops", "Specify the ratios for inserts/queries to perform; e.g. ops(insert=2,<query1>=1) will perform 2 inserts for each query1. When using multiple files, specify as keyspace.table.op.");
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return merge(Arrays.asList(ops, profile, clustering), parent.options());
//        }
//

    // CLI utility methods

    public static Options getOptions() {
        return new Options()
                .addOption(StressOption.COMMAND_CLUSTERING.option())
                .addOption(StressOption.COMMAND_PROFILE.option())
                .addOption(StressOption.COMMAND_RATIO.option());
    }

    public void printSettings(ResultLogger out)
    {
        super.printSettings(out);
        out.printf("  Command Ratios: %s%n", ratios);
        out.printf("  Command Clustering Distribution: %s%n", commandLine.getOptionValue(StressOption.COMMAND_CLUSTERING.option()));
        out.printf("  Profile File(s): %s%n", String.join(", ", commandLine.getOptionValues(StressOption.COMMAND_PROFILE.option())));
    }


//    public static SettingsCommandUser build(String[] params)
//    {
//        GroupedOptions options = GroupedOptions.select(params,
//                new Options(new Uncertainty()),
//                new Options(new Duration()),
//                new Options(new Count()));
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid USER options provided, see output for valid options");
//            System.exit(1);
//        }
//        return new SettingsCommandUser((Options) options);
//    }
//
//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "user",
//                                    new Options(new Uncertainty()),
//                                    new Options(new Count()),
//                                    new Options(new Duration()));
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
