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
package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Converter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SettingsSchema extends AbstractSettings implements Serializable
{
    public static final StressOption<Class<? extends AbstractReplicationStrategy>> SCHEMA_REP_STRATEGY = new StressOption<>(() -> org.apache.cassandra.locator.SimpleStrategy.class,
                                                                                                                            Option.builder("schema-replication")
                                                                                                                                  .desc( "The replication strategy class. (Default = SimpleStrategy)")
                                                                                                                                  .converter(AbstractReplicationStrategy::getClass).argName("class").hasArgs().build());

    public static final StressOption<Map<String,String>> SCHEMA_REP_ARGS = new StressOption<>(() -> Collections.emptyMap(),
                                                                                                                            Option.builder("schema-replication-args")
                                                                                                                                  .hasArgs()
                                                                                                                                  .valueSeparator()
                                                                                                                                  .desc( "The optional arguments for the replication strategy.  Must be followed by another option or '--'.")
                                                                                                                                  .build());
    public static final StressOption<Class<? extends AbstractCompactionStrategy>> SCHEMA_COMPACTION_STRATEGY = new StressOption<>(Option.builder("schema-compaction").converter(CompactionParams::classFromName).hasArg().argName("class or short name")
                                                                                                                                        .desc("The compaction strategy.")
                                                                                                                                        .build());

    public static final StressOption<Map<String,String>> SCHEMA_COMPACTION_ARGS = new StressOption<>(() -> Collections.emptyMap(),
                                                                                              Option.builder("schema-compaction-args")
                                                                                                    .hasArgs()
                                                                                                    .valueSeparator()
                                                                                                    .desc( "The optional arguments for the compaction strategy.  Must be followed by another option or '--'.")
                                                                                                    .hasArgs().build());

    public static final StressOption<String> SCHEMA_KEYSPACE = new StressOption<>(()->"keyspace1", new Option("schema-keyspace", true, "The keyspace name to use. (Default: keyspace1)"));
    public static final StressOption<String> SCHEMA_COMPRESSION = new StressOption<>(new Option("schema-compression", true, "Specify the compression to use for sstable. (Default: no-compression)"));



    final Class<? extends AbstractReplicationStrategy> replicationStrategy;
    final Map<String, String> replicationStrategyOptions;

    final String compression;
    final Class<? extends AbstractCompactionStrategy> compactionStrategy;
    final Map<String, String> compactionStrategyOptions;

    public final String keyspace;

    public SettingsSchema(CommandLine commandLine, SettingsCommand command)
    {
        if (command instanceof SettingsCommandUser)
        {
            for (Option option : getOptions().getOptions())
            {
                if (commandLine.hasOption(option))
                {
                    throw new IllegalArgumentException(String.format("Shema option %s can not be used with the 'user' Command", option.getKey()));
                }
            }
            keyspace = null; //this should never be used - StressProfile passes keyspace name directly
        } else
            keyspace = SCHEMA_KEYSPACE.extract(commandLine);

        replicationStrategy = SCHEMA_REP_STRATEGY.extract(commandLine);
        replicationStrategyOptions = SCHEMA_REP_ARGS.extractMap(commandLine);
        if (replicationStrategyOptions.get("replication_factor") == null)
        {
            replicationStrategyOptions.put("replication_factor", "1");
        }
        compression = SCHEMA_COMPRESSION.extract(commandLine);
        compactionStrategy = SCHEMA_COMPACTION_STRATEGY.extract(commandLine);
        compactionStrategyOptions = SCHEMA_COMPACTION_ARGS.extractMap(commandLine);
    }

    /**
     * Create Keyspace with Standard and Super/Counter column families
     */
    public void createKeySpaces(StressSettings settings)
    {

        JavaDriverClient client  = settings.getJavaDriverClient(false);

        try
        {
            //Keyspace
            client.execute(createKeyspaceStatementCQL3(), org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);

            client.execute("USE \""+keyspace+"\"", org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);

            //Add standard1 and counter1
            client.execute(createStandard1StatementCQL3(settings), org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);
            client.execute(createCounter1StatementCQL3(settings), org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE);

            System.out.println(String.format("Created keyspaces. Sleeping %ss for propagation.", settings.node.nodes.size()));
            Thread.sleep(settings.node.nodes.size() * 1000L); // seconds
        }
        catch (AlreadyExistsException e)
        {
            //Ok.
        }
        catch (Exception e)
        {
            throw new RuntimeException("Encountered exception creating schema", e);
        }
    }

    String createKeyspaceStatementCQL3()
    {
        StringBuilder b = new StringBuilder();

        //Create Keyspace
        b.append("CREATE KEYSPACE IF NOT EXISTS \"")
         .append(keyspace)
         .append("\" WITH replication = {'class': '")
         .append(replicationStrategy.getName())
         .append("', ")
         .append(optionsAsString(replicationStrategyOptions))
         .append("}  AND durable_writes = true;\n");

        return b.toString();
    }

    String createStandard1StatementCQL3(StressSettings settings)
    {

        StringBuilder b = new StringBuilder();

        b.append("CREATE TABLE IF NOT EXISTS ")
         .append("standard1 (key blob PRIMARY KEY ");

        try
        {
            for (ByteBuffer name : settings.columns.names)
                b.append("\n, \"").append(ByteBufferUtil.string(name)).append("\" blob");
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }

        return completeTable(b);
    }

    private String completeTable(StringBuilder b) {
        //Compression
        b.append(") WITH compression = {");
        if (compression != null)
            b.append("'class' : '").append(compression).append("'");

        b.append("}");

        //Compaction
        if (compactionStrategy != null)
        {
            b.append(" AND compaction = { 'class' : '").append(compactionStrategy.getName()).append("', ")
            .append(optionsAsString(compactionStrategyOptions))
            .append("}");
        }

        b.append(";\n");

        return b.toString();
    }

    String createCounter1StatementCQL3(StressSettings settings)
    {

        StringBuilder b = new StringBuilder();

        b.append("CREATE TABLE IF NOT EXISTS ")
         .append("counter1 (key blob PRIMARY KEY,");

        try
        {
            for (ByteBuffer name : settings.columns.names)
                b.append("\n, \"").append(ByteBufferUtil.string(name)).append("\" counter");
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }

        return completeTable(b);
    }

    // Option Declarations

    public static Options getOptions() {
        return new Options()
               .addOption(SCHEMA_COMPACTION_STRATEGY.option())
               .addOption(SCHEMA_COMPACTION_ARGS.option())
               .addOption(SCHEMA_REP_STRATEGY.option())
               .addOption(SCHEMA_REP_ARGS.option())
               .addOption(SCHEMA_COMPRESSION.option())
               .addOption(SCHEMA_KEYSPACE.option());
    }

//    private static final class Options extends GroupedOptions
//    {
//        final OptionReplication replication = new OptionReplication();
//        final OptionCompaction compaction = new OptionCompaction();
//        final OptionSimple keyspace = new OptionSimple("keyspace=", ".*", "keyspace1", "The keyspace name to use", false);
//        final OptionSimple compression = new OptionSimple("compression=", ".*", null, "Specify the compression to use for sstable, default:no compression", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(replication, keyspace, compaction, compression);
//        }
//    }

    private String optionsAsString(Map<String,String> options) {
        return String.join(", ", options.entrySet().stream().map( e -> String.format("'%s' : '%s'", e.getKey(), e.getValue())).collect(Collectors.toSet()));
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.println("  Keyspace: " + keyspace);
        out.println("  Replication Strategy: " + replicationStrategy);
        out.println("  Replication Strategy Options: " + optionsAsString(replicationStrategyOptions));

        out.println("  Table Compression: " + compression);
        out.println("  Table Compaction Strategy: " + compactionStrategy);
        out.println("  Table Compaction Strategy Options: " + optionsAsString(compactionStrategyOptions));
    }

//
//    public static SettingsSchema get(Map<String, String[]> clArgs, SettingsCommand command)
//    {
//        String[] params = clArgs.remove("-schema");
//        if (params == null)
//            return new SettingsSchema(new Options(), command);
//
//        if (command instanceof SettingsCommandUser)
//            throw new IllegalArgumentException("-schema can only be provided with predefined operations insert, read, etc.; the 'user' command requires a schema yaml instead");
//
//        GroupedOptions options = GroupedOptions.select(params, new Options());
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -schema options provided, see output for valid options");
//            System.exit(1);
//        }
//        return new SettingsSchema((Options) options, command);
//    }

//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-schema", new Options());
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
