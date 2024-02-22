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


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.datastax.driver.core.BatchStatement;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.RatioDistributionFactory;
import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;

public class SettingsInsert extends AbstractSettings
{

    public static final StressOption<DistributionFactory> INSERT_PARTITIONS = new StressOption<>(Option.builder("insert-partitions").hasArg().type(DistributionFactory.class)
                                                                                                       .desc("The number of partitions to update in a single batch").hasArg()
                                                                                                       .build());
    public static final StressOption<BatchStatement.Type>  INSERT_BATCH_TYPE = new StressOption<>(Option.builder("insert-batchtype").hasArg()
                                                                                                        .converter(s ->BatchStatement.Type.valueOf(s.toUpperCase()))
                                                                                                        .desc(format("Specify the type of batch statement. Valid values are %s.", enumOptionString(BatchStatement.Type.UNLOGGED))).build());
    public static final StressOption<RatioDistributionFactory> INSERT_SELECT_RATIO = new StressOption<>(Option.builder("insert-select-ratio")
                                                                                                              .desc("The uniform probability of visiting any CQL row in the generated partition.")
                                                                                                        .hasArg().type(RatioDistributionFactory.class).converter(RATIO_DISTRIBUTION_FACTORY_CONVERTER).build());
    private static final String INSERT_VISITS_DEFAULT = "FIXED(1)";
    public static final StressOption<DistributionFactory> INSERT_VISITS = new StressOption<>(()->OptionDistribution.get(INSERT_VISITS_DEFAULT),
                                                                                             Option.builder("insert-visits").hasArg().type(DistributionFactory.class)
                                                                                                   .desc(format("The target number of inserts to split a partition into; if more than one, the partition will be placed in the revisit set. (Default %s)", INSERT_VISITS_DEFAULT)).build());
    private static final String INSERT_REVISIT_DEFAULT = "UNIFORM(1..1M)";
    public static final StressOption<DistributionFactory> INSERT_REVISIT = new StressOption<>(()->OptionDistribution.get(INSERT_REVISIT_DEFAULT),
                                                                                              Option.builder("insert-revisit").hasArg().type(DistributionFactory.class)
                                                                                                    .desc(format("The distribution with which we revisit partial writes (see %s); implicitly defines size of revisit collection. (Default %s)", INSERT_VISITS.key(), INSERT_REVISIT_DEFAULT)).build());
    private static final String INSERT_ROW_POPULATION_RATIO_DEFAULT = "FIXED(1)/1";
    public static final StressOption<RatioDistributionFactory> INSERT_ROW_POPULATION_RATIO = new StressOption<>(()-> {try

        {
            return RATIO_DISTRIBUTION_FACTORY_CONVERTER.apply(INSERT_ROW_POPULATION_RATIO_DEFAULT);
        }catch (Exception e) {
            throw asRuntimeException(e);
        }},
                                                                                                                Option.builder("insert-row-population-ratio")
                                                                                                                      .desc(format("The percent of a given rows columns to populate. (default %s)",INSERT_ROW_POPULATION_RATIO_DEFAULT))
                                                                                                                      .hasArg().type(RatioDistributionFactory.class).converter(RATIO_DISTRIBUTION_FACTORY_CONVERTER).build());
    public final DistributionFactory revisit;
    public final DistributionFactory visits;
    public final DistributionFactory batchsize;
    public final RatioDistributionFactory selectRatio;
    public final RatioDistributionFactory rowPopulationRatio;
    public final BatchStatement.Type batchType;

    public SettingsInsert(CommandLine commandLine)
    {
            this.visits = INSERT_VISITS.extract(commandLine);
            this.revisit =  INSERT_REVISIT.extract(commandLine);
            this.batchsize = INSERT_PARTITIONS.extract(commandLine);
            this.selectRatio = INSERT_SELECT_RATIO.extract(commandLine);
            this.rowPopulationRatio = INSERT_ROW_POPULATION_RATIO.extract(commandLine);
            this.batchType = INSERT_BATCH_TYPE.extract(commandLine);

    }

    // Option Declarations

    public static Options getOptions() {
        return new Options()
               .addOption(INSERT_VISITS.option())
               .addOption(INSERT_REVISIT.option())
               .addOption(INSERT_PARTITIONS.option())
               .addOption(INSERT_BATCH_TYPE.option())
               .addOption(INSERT_SELECT_RATIO.option())
               .addOption(INSERT_ROW_POPULATION_RATIO.option())
        ;
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {

        if (revisit != null)
        {
            out.println("  Revisits: " +revisit.getConfigAsString());
        }
        if (visits != null)
        {
            out.println("  Visits: " + visits.getConfigAsString());
        }
        if (batchsize != null)
        {
            out.println("  Batchsize: " +batchsize.getConfigAsString());
        }
        if (selectRatio != null)
        {
            out.println("  Select Ratio: " +selectRatio.getConfigAsString());
        }
        if (rowPopulationRatio != null)
        {
            out.println("  Row Population Ratio: " +rowPopulationRatio.getConfigAsString());
        }
        if (batchType != null)
        {
            out.printf("  Batch Type: %s%n", batchType);
        } else {
            out.println("  Batch Type: not batching");
        }
    }
}

