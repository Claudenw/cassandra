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

import com.google.common.primitives.Ints;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.cassandra.stress.util.ResultLogger;

public class SettingsTokenRange extends AbstractSettings implements Serializable
{
    public static final StressOption<String> TOKEN_RANGE_WRAP = new StressOption<>(new Option("token-range-wrap", "Re-use token ranges in order to terminate stress iterations"));
    public static final StressOption<Integer> TOKEN_RANGE_SPLIT_FACTORY = new StressOption<>(()->1, Option.builder("token-range-split").hasArg().desc("Split every token range by this factor")
                                                                                                          .converter(s -> Ints.checkedCast(OptionDistribution.parseLong(s))).build());
    public final boolean wrap;
    public final int splitFactor;
    //private final CommandLine options;

    public SettingsTokenRange(CommandLine commandLine)
    {
       // this.options = commandLine;
        this.wrap = commandLine.hasOption(TOKEN_RANGE_WRAP.option());
        this.splitFactor = TOKEN_RANGE_SPLIT_FACTORY.extract(commandLine);
    }

    public static Options getOptions()
    {
        return new Options()
               .addOption(TOKEN_RANGE_SPLIT_FACTORY.option())
               .addOption(TOKEN_RANGE_WRAP.option());
    }

//    private static final class TokenRangeOptions extends GroupedOptions
//    {
//        final OptionSimple wrap = new OptionSimple("wrap", "", null, "Re-use token ranges in order to terminate stress iterations", false);
//        final OptionSimple splitFactor = new OptionSimple("split-factor=", "[0-9]+[bmk]?", "1", "Split every token range by this factor", false);
//
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return ImmutableList.<Option>builder().add(wrap, splitFactor).build();
//        }
//    }

//    public static SettingsTokenRange get(Map<String, String[]> clArgs)
//    {
//        String[] params = clArgs.remove("-tokenrange");
//        if (params == null)
//        {
//            return new SettingsTokenRange(new TokenRangeOptions());
//        }
//        TokenRangeOptions options = GroupedOptions.select(params, new TokenRangeOptions());
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -tokenrange options provided, see output for valid options");
//            System.exit(1);
//        }
//        return new SettingsTokenRange(options);
//    }

    public void printSettings(ResultLogger out)
    {
        out.printf("  Wrap: %b%n", wrap);
        out.printf("  Split Factor: %d%n", splitFactor);
    }
//
//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-tokenrange", new TokenRangeOptions());
//    }
//
//    public static Runnable helpPrinter()
//    {
//        return SettingsTokenRange::printHelp;
//    }
}
