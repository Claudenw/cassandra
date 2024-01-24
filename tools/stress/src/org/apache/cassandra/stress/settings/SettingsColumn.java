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

import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.stress.generate.Distribution;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.DistributionFixed;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

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
            sizeDistribution = cmdLine.getParsedOptionValue(StressOption.COL_COUNT.option(), StressOption.COL_COUNT.dfltSupplier());
            {
                timestamp = cmdLine.getOptionValue(StressOption.COL_TIMESTAMP.option());
                comparator = cmdLine.getOptionValue(StressOption.COL_COMPARATOR.option(), (String)StressOption.COL_COMPARATOR.dfltSupplier().get());
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
            if (cmdLine.hasOption(StressOption.COL_NAMES.option()))
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

                final String[] names = cmdLine.getOptionValue(StressOption.COL_NAMES.option()).split(",");
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
                        return format("Count:  fixed=%d", nameCount);
                    }
                };
            }
            else
            {
                this.countDistribution = cmdLine.getParsedOptionValue(StressOption.COL_COUNT.option(), StressOption.COL_COUNT.dfltSupplier());
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
            slice = cmdLine.hasOption(StressOption.COL_SLICE.option());
        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }

    // Option Declarations



    public static Options getOptions() {

        Options result = new Options()
                .addOption(StressOption.COL_SUPER.option())
                .addOption(StressOption.COL_COMPARATOR.option())
                .addOption(StressOption.COL_SLICE.option())
                .addOption(StressOption.COL_TIMESTAMP.option())
                .addOption(StressOption.COL_SIZE.option());
        OptionGroup COUNT_OR_NAMES = new OptionGroup()
                .addOption(StressOption.COL_NAMES.option())
                .addOption(StressOption.COL_COUNT.option());

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

}
