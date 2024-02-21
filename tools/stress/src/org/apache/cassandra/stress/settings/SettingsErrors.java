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


import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class SettingsErrors extends AbstractSettings
{

    public static final StressOption<String> ERROR_IGNORE = new StressOption<>(new Option("error-ignore", "Do not fail on errors."));
    public static final StressOption<String> SKIP_READ_VALIDATION = new StressOption<>(new Option("skip-read-validation", "Skip read validation and message output."));
    public static final StressOption<Integer> ERROR_RETRIES = new StressOption<>(()->9, POSITIVE_VERIFIER, Option.builder("retries").hasArg().type(Integer.class).desc("Number of tries to perform for each operation before failing.").build());
    public final boolean ignore;
    public final int tries;
    public final boolean skipReadValidation;

    public SettingsErrors(CommandLine commandLine)
    {
        try {
            ignore = commandLine.hasOption(ERROR_IGNORE.option());
            tries = ERROR_RETRIES.extract(commandLine) + 1;
            skipReadValidation = commandLine.hasOption(SKIP_READ_VALIDATION.option());
        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }

    // Option Declarations
    public static Options getOptions()
    {
        return new Options()
               .addOption(ERROR_IGNORE.option())
               .addOption(ERROR_RETRIES.option())
               .addOption(SKIP_READ_VALIDATION.option());
    }


    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Ignore: %b%n", ignore);
        out.printf("  Tries: %d%n", tries);
        out.printf("  Skip validation: %b%n", skipReadValidation);
    }
}
