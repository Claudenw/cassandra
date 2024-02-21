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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

/**
 * For parsing column options
 */
public class SettingsColumn extends AbstractSettings implements Serializable
{

    private enum AcceptableComparators {TimeUUIDType, AsciiType, UTF8Type }

    public static final StressOption<String> COL_COMPARATOR = new StressOption<>(()->AcceptableComparators.AsciiType.name(),
                                                                                 enumVerifier(AcceptableComparators.AsciiType),
                                                                                 Option.builder("col-comparator").hasArg()
                                                                                    .desc(format("Column Comparator to use.  Only applicable if -col-names is specified. Valid values are: %s (Default %s)",enumOptionString(AcceptableComparators.AsciiType), "AsciiType")).build());

    public static final StressOption<String> COL_SLICE = new StressOption<>(new Option("col-slice", "If set, range slices will be used for reads, otherwise a names query is used."));
    public static final StressOption<String> COL_TIMESTAMP = new StressOption<>(Option.builder("col-timestamp").hasArg().desc("If set, all columns will be written with the given timestamp.").build());
    public static final StressOption<String> COL_NAMES = new StressOption<>(new Option("col-names", true, "A comma separated list of column names.  May not be used with -column-count."));
    private static final String COL_SIZE_DEFAULT="FIXED(34)";
    public static final StressOption<DistributionFactory> COL_SIZE = new StressOption<>(()->OptionDistribution.get(COL_SIZE_DEFAULT), Option.builder("col-size").hasArg().type(DistributionFactory.class).desc(format("Cell size distribution. (Default %s)", COL_SIZE_DEFAULT)).build());
    private static final String COL_COUNT_DEFAULT = "FIXED(5)";
    public static final StressOption<DistributionFactory>  COL_COUNT = new StressOption<>(()->OptionDistribution.get(COL_COUNT_DEFAULT), Option.builder("col-count").hasArg().type(DistributionFactory.class)
                                                                                                                                               .desc(format("Cell count distribution, per operation (Default %s).  May not be used with -col-names.", COL_COUNT_DEFAULT)).build());
    public final int maxColumnsPerKey;
    public transient List<ByteBuffer> names;
    public final List<String> namestrs;
    final AbstractType<?> comparator;
    public final String timestamp;
    public final boolean variableColumnCount;
    public final boolean slice;
    public final DistributionFactory sizeDistribution;
    public final DistributionFactory countDistribution;

    public SettingsColumn(CommandLine cmdLine)
    {
        try
        {
            sizeDistribution = COL_SIZE.extract(cmdLine);
            timestamp = COL_TIMESTAMP.extract(cmdLine);
            if (cmdLine.hasOption(COL_NAMES.option()))
            {
                try
                {
                    this.comparator = TypeParser.parse(COL_COMPARATOR.extract(cmdLine));
                }
                catch (Exception e)
                {
                    // SHOULD not occur
                    throw new IllegalArgumentException(cmdLine.getOptionValue(COL_COMPARATOR.option()) + " is not a valid type", e);
                }

                final String[] names = cmdLine.getOptionValue(COL_NAMES.option()).split(",");
                this.names = new ArrayList<>(names.length);

                for (String columnName : names)
                    this.names.add(this.comparator.fromString(columnName));
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
                this.comparator = null;
                this.countDistribution = COL_COUNT.extract(cmdLine);
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
            slice = cmdLine.hasOption(COL_SLICE.option());
        } catch (Exception e) {
            throw asRuntimeException(e);
        }
    }

    /**
     * The options generated and used by this group of settings.
     * @return the options for this group of settings.
     */
    public static Options getOptions() {

        Options result = new Options()
                .addOption(COL_COMPARATOR.option())
                .addOption(COL_SLICE.option())
                .addOption(COL_TIMESTAMP.option())
                .addOption(COL_SIZE.option());
        OptionGroup COUNT_OR_NAMES = new OptionGroup()
                .addOption(COL_NAMES.option())
                .addOption(COL_COUNT.option());

        result.addOptionGroup(COUNT_OR_NAMES);
        return result;
    }

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
