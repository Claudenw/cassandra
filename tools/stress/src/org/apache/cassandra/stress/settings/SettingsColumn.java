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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.DistributionFixed;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.converters.Verifier;

/**
 * For parsing column options
 */
public class SettingsColumn extends AbstractSettings implements Serializable
{
    public final int maxColumnsPerKey;
    public transient List<ByteBuffer> names;
    public final List<String> namestrs;
    public final String comparator;
    public final String timestamp;
    public final boolean variableColumnCount;
    public final boolean slice;
    public final DistributionFactory sizeDistribution;
    public final DistributionFactory countDistribution;


//    public SettingsColumn(GroupedOptions options)
//    {
//        this((Options) options,
//                options instanceof NameOptions ? (NameOptions) options : null,
//                options instanceof CountOptions ? (CountOptions) options : null
//        );
//    }

    public SettingsColumn(CommandLine cmdLine)
    {
        try
        {
            sizeDistribution = cmdLine.getParsedOptionValue(SIZE, SIZE_DEFAULT);
            {
                timestamp = cmdLine.getOptionValue(TIMESTAMP);
                comparator = cmdLine.getOptionValue(COMPARATOR, COMPARATOR_DEFAULT);
//            -- THE following is replaced by the verifier
//            AbstractType parsed = null;
//
//            try
//            {
//                parsed = TypeParser.parse(comparator);
//            }
//            catch (Exception e)
//            {
//                System.err.println(e.getMessage());
//                System.exit(1);
//            }
//
//            if (!(parsed instanceof TimeUUIDType || parsed instanceof AsciiType || parsed instanceof UTF8Type))
//            {
//                System.err.println("Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
//                System.exit(1);
//            }1
            }
            if (cmdLine.hasOption(NAMES))
            {
                AbstractType comparator;
                try
                {
                    comparator = TypeParser.parse(this.comparator);
                }
                catch (Exception e)
                {
                    // SHOULD not occur
                    throw new IllegalArgumentException(this.comparator + " is not a valid type");
                }

                final String[] names = cmdLine.getOptionValue(NAMES).split(",");
                this.names = new ArrayList<>(names.length);

                for (String columnName : names)
                    this.names.add(comparator.fromString(columnName));
                Collections.sort(this.names, BytesType.instance);
                this.namestrs = new ArrayList<>();
                for (ByteBuffer columnName : this.names)
                    this.namestrs.add(comparator.getString(columnName));

                final int nameCount = this.names.size();
                countDistribution = new DistributionFactory()
                {
                    @Override
                    public Distribution get()
                    {
                        return new DistributionFixed(nameCount);
                    }

                    @Override
                    public String getConfigAsString()
                    {
                        return String.format("Count:  fixed=%d", nameCount);
                    }
                };
            }
            else
            {
                this.countDistribution = cmdLine.getParsedOptionValue(COUNT, COUNT_DEFAULT);
                ByteBuffer[] names = new ByteBuffer[(int) countDistribution.get().maxValue()];
                String[] namestrs = new String[(int) countDistribution.get().maxValue()];
                for (int i = 0; i < names.length; i++)
                    names[i] = ByteBufferUtil.bytes("C" + i);
                Arrays.sort(names, BytesType.instance);
                try
                {
                    for (int i = 0; i < names.length; i++)
                        namestrs[i] = ByteBufferUtil.string(names[i]);
                }
                catch (CharacterCodingException e)
                {
                    throw new RuntimeException(e);
                }
                this.names = Arrays.asList(names);
                this.namestrs = Arrays.asList(namestrs);
            }
            maxColumnsPerKey = (int) countDistribution.get().maxValue();
            variableColumnCount = countDistribution.get().minValue() < maxColumnsPerKey;
            slice = cmdLine.hasOption(SLICE);
        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }

    // Option Declarations


   private static final String SUPER_COLUMNS="-col-super";
    private static final int SUPER_COLUMN_DEFAULT=0;
    private static final String COMPARATOR="-col-comparator";
    private static final String COMPARATOR_DEFAULT = "AsciiType";

    private enum AcceptableComparators {TimeUUIDType, AsciiType, UTF8Type };
    private static final Verifier COMPARATOR_VERIFIER = new EnumVerifier(AcceptableComparators.AsciiType);
    private static final String SLICE="-col-slice";
    private static final String TIMESTAMP="-col-timestamp";
    private static final String SIZE="-col-size";
    private static final DistributionFactory SIZE_DEFAULT = OptionDistribution.get("FIXED(34)");

    private static final String NAMES="-col-names";

    private static final String COUNT="-col-count";
    private static final DistributionFactory COUNT_DEFAULT = OptionDistribution.get("FIXED(5)");

    private static OptionGroup COUNT_OR_NAMES;
    public static Options getOptions() {
     String validComparatorStr = String.join(", ", Arrays.stream(AcceptableComparators.values()).map(AcceptableComparators::name).collect(Collectors.toList()));

     Options result = new Options()
        .addOption( org.apache.commons.cli.Option.builder(SUPER_COLUMNS).hasArg().type(Integer.class).desc("Number of super columns to use (no super columns used if not specified)").build())
        .addOption( org.apache.commons.cli.Option.builder(COMPARATOR).hasArg().verifier(COMPARATOR_VERIFIER)
                          .desc("Column Comparator to use. Valid values are: "+validComparatorStr).build())
        .addOption( new org.apache.commons.cli.Option(SLICE, "If set, range slices will be used for reads, otherwise a names query will be"))
        .addOption( org.apache.commons.cli.Option.builder(TIMESTAMP).hasArg().verifier(Verifier.INTEGER).desc("If set, all columns will be written with the given timestamp").build())
        .addOption( org.apache.commons.cli.Option.builder(SIZE).hasArg().type(DistributionFactory.class).desc("Cell size distribution").build());

        OptionGroup COUNT_OR_NAMES = new OptionGroup()
     .addOption(  new org.apache.commons.cli.Option(NAMES, true, "Column names"))

     .addOption(org.apache.commons.cli.Option.builder(COUNT).hasArg().type(DistributionFactory.class).desc("Cell count distribution, per operation").build());
     result.addOptionGroup(COUNT_OR_NAMES);

        return result;
    }

//    private static abstract class Options extends GroupedOptions
//    {
//        final OptionSimple superColumns = new OptionSimple("super=", "[0-9]+", "0", "Number of super columns to use (no super columns used if not specified)", false);
//        final OptionSimple comparator = new OptionSimple("comparator=", "TimeUUIDType|AsciiType|UTF8Type", "AsciiType", "Column Comparator to use", false);
//        final OptionSimple slice = new OptionSimple("slice", "", null, "If set, range slices will be used for reads, otherwise a names query will be", false);
//        final OptionSimple timestamp = new OptionSimple("timestamp=", "[0-9]+", null, "If set, all columns will be written with the given timestamp", false);
//        final OptionDistribution size = new OptionDistribution("size=", "FIXED(34)", "Cell size distribution");
//    }

//    private static final class NameOptions extends Options
//    {
//        final OptionSimple name = new OptionSimple("names=", ".*", null, "Column names", true);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(name, slice, superColumns, comparator, timestamp, size);
//        }
//    }

//    private static final class CountOptions extends Options
//    {
//        final OptionDistribution count = new OptionDistribution("n=", "FIXED(5)", "Cell count distribution, per operation");
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(count, slice, superColumns, comparator, timestamp, size);
//        }
//    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Max Columns Per Key: %d%n",maxColumnsPerKey);
        out.printf("  Column Names: %s%n",namestrs);
        out.printf("  Comparator: %s%n", comparator);
        out.printf("  Timestamp: %s%n", timestamp);
        out.printf("  Variable Column Count: %b%n", variableColumnCount);
        out.printf("  Slice: %b%n", slice);
        if (sizeDistribution != null){
            out.println("  Size Distribution: " + sizeDistribution.getConfigAsString());
        };
        if (sizeDistribution != null){
            out.println("  Count Distribution: " + countDistribution.getConfigAsString());
        };
    }


    static SettingsColumn get(CommandLine cmdLine)
    {
//
//        if (params == null)
//            return new SettingsColumn(new CountOptions());
//
//        GroupedOptions options = GroupedOptions.select(params, new NameOptions(), new CountOptions());
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -col options provided, see output for valid options");
//            System.exit(1);
//        }
        return new SettingsColumn(cmdLine);
    }

//    static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-col", new NameOptions(), new CountOptions());
//    }
//
//    static Runnable helpPrinter()
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

    /* Custom serializaiton invoked here to make legacy thrift based table creation work with StressD. This code requires
     * the names attribute to be populated. Since the names attribute is set as a List[ByteBuffer] we switch it
     * to an array on the way out and back to a buffer when it's being read in.
     */
// TODO is this needed?
    private void writeObject(ObjectOutputStream oos) throws IOException
    {
        oos.defaultWriteObject();
        ArrayList<byte[]> namesBytes = new ArrayList<>();
        for (ByteBuffer buffer : this.names)
            namesBytes.add(ByteBufferUtil.getArray(buffer));
        oos.writeObject(namesBytes);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException
    {
        ois.defaultReadObject();
        List<ByteBuffer> namesBuffer = new ArrayList<>();
        List<byte[]> namesBytes = (List<byte[]>) ois.readObject();
        for (byte[] bytes : namesBytes)
            namesBuffer.add(ByteBuffer.wrap(bytes));
        this.names = new ArrayList<>(namesBuffer);
    }

    public static void main(String[] args) {
        Options options = getOptions();
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("myapp", "HEADER", options, "FOOTER", true);
    }
}
