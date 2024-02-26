/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.stress.settings;

import java.io.Serializable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsReporting extends AbstractSettings implements Serializable
{
    public static final StressOption<DurationSpec.IntSecondsBound> REPORTING_HEADER_FREQ = new StressOption<>(()->new DurationSpec.IntSecondsBound(0),
                                                                                                              Option.builder("reporting-header-freq").hasArg().type(DurationSpec.IntSecondsBound.class)
                                                                                                                    .desc("Frequency the header for the statistics will be printed out. " +
                                                                                                                                             "If not specified, the header will be printed at the beginning of the test only.").build());
    public static final StressOption<DurationSpec.IntSecondsBound> REPORTING_OUTPUT_FREQ = new StressOption<>(()->new DurationSpec.IntSecondsBound(1),
                                                                                                              Option.builder("reporting-output-freq").hasArg().type(DurationSpec.IntSecondsBound.class)
                                                                                                                    .desc("Frequency each line of output will be printed out when running a stress test. (Defai;t = 1s)").build());
    public final  DurationSpec.IntSecondsBound  outputFrequency;

    public final DurationSpec.IntSecondsBound  headerFrequency;


    public SettingsReporting(CommandLine commandLine)
    {
        headerFrequency = REPORTING_HEADER_FREQ.extract(commandLine);

        outputFrequency = REPORTING_OUTPUT_FREQ.extract(commandLine);
    }

    // Option Declarations

    public static Options getOptions()
    {
        return new Options()
               .addOption(REPORTING_HEADER_FREQ.option())
               .addOption(REPORTING_OUTPUT_FREQ.option());
    }

    public void printSettings(ResultLogger out)
    {
        out.printf("  Output frequency: %s%n", outputFrequency.toString());
        out.printf("  Header frequency: %s%n", headerFrequency.toString());
    }

}
