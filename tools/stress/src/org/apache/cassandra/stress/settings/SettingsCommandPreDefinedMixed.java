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


import java.util.*;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.SeedManager;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.apache.cassandra.stress.operations.SampledOpDistributionFactory;
import org.apache.cassandra.stress.operations.predefined.PredefinedOperation;
import org.apache.cassandra.stress.report.Timer;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;


// Settings unique to the mixed command type
public class SettingsCommandPreDefinedMixed extends SettingsCommandPreDefined
{

    // Ratios for selecting commands - index for each Command, NaN indicates the command is not requested
    final Map<Command, Double> ratios;
    final DistributionFactory clustering;

    private final CommandLine commandLine;


    public SettingsCommandPreDefinedMixed(CommandLine commandLine)
    {
        super(Command.MIXED, commandLine);

        try {
            clustering = COMMAND_CLUSTERING.extract(commandLine);
            if (commandLine.hasOption(COMMAND_RATIO.option())) {
                ratios = COMMAND_RATIO.extractMap(commandLine, k->Command.valueOf(k.toUpperCase()), Double::parseDouble);
                if (ratios.size() == 0)
                    throw new IllegalArgumentException("Must specify at least one command with a non-zero ratio");
            } else {
                ratios = new HashMap<>();
                ratios.put(Command.WRITE, 1.0);
                ratios.put(Command.READ, 1.0);
            }
            this.commandLine = commandLine;
        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }

    public OpDistributionFactory getFactory(final StressSettings settings)
    {
        final SeedManager seeds = new SeedManager(settings);
        return new SampledOpDistributionFactory<Command>(ratios, clustering)
        {
            protected List<? extends Operation> get(Timer timer, Command key, boolean isWarmup)
            {
                return Collections.singletonList(PredefinedOperation.operation(key, timer, SettingsCommandPreDefinedMixed.this.newGenerator(settings), seeds, settings, add));
            }
        };
    }

    // Option Declarations
    public static Options getOptions()
    {
        return SettingsCommand.getOptions()
                .addOption(COMMAND_CLUSTERING.option())
                .addOption(COMMAND_RATIO.option());
    }

    // CLI utility methods

    public void printSettings(ResultLogger out)
    {
        super.printSettings(out);
        out.printf("  Command Ratios: %s%n", ratios);
        out.printf("  Command Clustering Distribution: %s%n", clustering.getConfigAsString());
    }
}
