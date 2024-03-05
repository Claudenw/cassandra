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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SettingsSchemaTests
{
    SettingsCommand getSettingsCommand(Command type, String... args) throws ParseException
    {
        return SettingsCommandTests.getInstance(type, SettingsCommand.getOptions(), args);
    }

    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertNull(underTest.compression);
        assertEquals(org.apache.cassandra.locator.SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(1, underTest.replicationStrategyOptions.size());
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: keyspace1");
        logger.assertEndsWith("Replication Strategy: class org.apache.cassandra.locator.SimpleStrategy");
        logger.assertEndsWith("Replication Strategy Options: 'replication_factor' : '1'");
        logger.assertEndsWith("Table Compression: null");
        logger.assertEndsWith("Table Compaction Strategy: null");
        logger.assertEndsWith("Table Compaction Strategy Options: ");
    }

    @Test
    public void replicationStrategyTest() throws ParseException
    {
        String[] args = {SettingsSchema.SCHEMA_REP_STRATEGY.key(), "LocalStrategy"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertNull(underTest.compression);
        assertEquals(LocalStrategy.class, underTest.replicationStrategy);
        assertEquals(1, underTest.replicationStrategyOptions.size());
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: keyspace1");
        logger.assertEndsWith("Replication Strategy: class org.apache.cassandra.locator.LocalStrategy");
        logger.assertEndsWith("Replication Strategy Options: 'replication_factor' : '1'");
        logger.assertEndsWith("Table Compression: null");
        logger.assertEndsWith("Table Compaction Strategy: null");
        logger.assertEndsWith("Table Compaction Strategy Options: ");
    }

    @Test
    public void replicationArgumentsTest() throws ParseException
    {
        String[] args = {SettingsSchema.SCHEMA_REP_ARGS.key(), "foo=bar", "fu=baz"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertNull(underTest.compression);
        assertEquals(org.apache.cassandra.locator.SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(3, underTest.replicationStrategyOptions.size());
        assertEquals("bar", underTest.replicationStrategyOptions.get("foo"));
        assertEquals("baz", underTest.replicationStrategyOptions.get("fu"));
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: keyspace1");
        logger.assertEndsWith("Replication Strategy: class org.apache.cassandra.locator.SimpleStrategy");
        logger.assertContainsRegex("Replication Strategy Options:.+'fu' : 'baz'");
        logger.assertContainsRegex("Replication Strategy Options:.+'replication_factor' : '1'");
        logger.assertContainsRegex("Replication Strategy Options:.+'foo' : 'bar'");
        logger.assertEndsWith("Table Compression: null");
        logger.assertEndsWith("Table Compaction Strategy: null");
        logger.assertEndsWith("Table Compaction Strategy Options: ");
    }

    @Test
    public void compactionStrategyTest() throws ParseException
    {
        String[] args = {SettingsSchema.SCHEMA_COMPACTION_STRATEGY.key(), "LeveledCompactionStrategy"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertEquals(LeveledCompactionStrategy.class, underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertNull(underTest.compression);
        assertEquals(org.apache.cassandra.locator.SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(1, underTest.replicationStrategyOptions.size());
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: keyspace1");
        logger.assertEndsWith("Replication Strategy: class org.apache.cassandra.locator.SimpleStrategy");
        logger.assertEndsWith("Replication Strategy Options: 'replication_factor' : '1'");
        logger.assertEndsWith("Table Compression: null");
        logger.assertEndsWith("Table Compaction Strategy: class org.apache.cassandra.db.compaction.LeveledCompactionStrategy");
        logger.assertEndsWith("Table Compaction Strategy Options: ");    }

    @Test
    public void compactionArgumentsTest() throws ParseException
    {
        try
        {
            String[] args = {SettingsSchema.SCHEMA_COMPACTION_ARGS.key(), "foo=bar", "fu=baz" };
            CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
            SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // do nothing;
        }

        String[] args = {SettingsSchema.SCHEMA_COMPACTION_STRATEGY.key(), "LeveledCompactionStrategy", SettingsSchema.SCHEMA_COMPACTION_ARGS.key(), "foo=bar", "fu=baz" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertEquals(LeveledCompactionStrategy.class, underTest.compactionStrategy);
        assertEquals(SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(2, underTest.compactionStrategyOptions.size());
        assertEquals("bar", underTest.compactionStrategyOptions.get("foo"));
        assertEquals("baz", underTest.compactionStrategyOptions.get("fu"));
        assertNull(underTest.compression);
        assertEquals(SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(1, underTest.replicationStrategyOptions.size());
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: keyspace1");
        logger.assertEndsWith("Replication Strategy: "+SimpleStrategy.class);
        logger.assertEndsWith("Replication Strategy Options: 'replication_factor' : '1'");
        logger.assertEndsWith("Table Compression: null");
        logger.assertEndsWith("Table Compaction Strategy: "+LeveledCompactionStrategy.class);
        logger.assertContainsRegex("Table Compaction Strategy Options:.+'fu' : 'baz'");
        logger.assertContainsRegex("Table Compaction Strategy Options:.+'foo' : 'bar'");
    }

    @Test
    public void keyspaceTest() throws ParseException
    {
        String[] args = {SettingsSchema.SCHEMA_KEYSPACE.key(), "ks1"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "ks1", underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertNull(underTest.compression);
        assertEquals(org.apache.cassandra.locator.SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(1, underTest.replicationStrategyOptions.size());
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: ks1");
        logger.assertEndsWith("Replication Strategy: class org.apache.cassandra.locator.SimpleStrategy");
        logger.assertEndsWith("Replication Strategy Options: 'replication_factor' : '1'");
        logger.assertEndsWith("Table Compression: null");
        logger.assertEndsWith("Table Compaction Strategy Options: ");

    }

    @Test
    public void compressionTest() throws ParseException
    {
        String[] args = {SettingsSchema.SCHEMA_COMPRESSION.key(), "foo"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertEquals("foo", underTest.compression);
        assertEquals(org.apache.cassandra.locator.SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(1, underTest.replicationStrategyOptions.size());
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: keyspace1");
        logger.assertEndsWith("Replication Strategy: class org.apache.cassandra.locator.SimpleStrategy");
        logger.assertEndsWith("Replication Strategy Options: 'replication_factor' : '1'");
        logger.assertEndsWith("Table Compression: foo");
        logger.assertEndsWith("Table Compaction Strategy: null");
        logger.assertEndsWith("Table Compaction Strategy Options: ");
    }

    @Test
    public void schemaOptionsWithUserCommandTest() throws ParseException, IOException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, SettingsCommandUserTest.getMinimalConfig());
        assertNull(underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertNull(underTest.compression);
        assertEquals(org.apache.cassandra.locator.SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(1, underTest.replicationStrategyOptions.size());
        assertEquals("1", underTest.replicationStrategyOptions.get("replication_factor"));

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Keyspace: *not set*");
        logger.assertEndsWith("Replication Strategy: class org.apache.cassandra.locator.SimpleStrategy");
        logger.assertEndsWith("Replication Strategy Options: 'replication_factor' : '1'");
        logger.assertEndsWith("Table Compression: null");
        logger.assertEndsWith("Table Compaction Strategy: null");
        logger.assertEndsWith("Table Compaction Strategy Options: ");
    }

    @Test
    public void createKeyspacesTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        StressSettingsTest.StressSettingsMockJavaDriver mockedStress = new StressSettingsTest.StressSettingsMockJavaDriver("HELP");
        List<String> expected = List.of(underTest.createKeyspaceStatementCQL3(),  "USE \"keyspace1\"", underTest.createStandard1StatementCQL3(mockedStress), underTest.createCounter1StatementCQL3(mockedStress));

        underTest.createKeySpaces(mockedStress);

        ArgumentCaptor<String > cmdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ConsistencyLevel> levelCaptor = ArgumentCaptor.forClass(ConsistencyLevel.class);
        verify(mockedStress.mockDriver, times(4)).execute(cmdCaptor.capture(), levelCaptor.capture());
        assertEquals( expected, cmdCaptor.getAllValues());
        Set<ConsistencyLevel> set = new HashSet<>();
        set.addAll(levelCaptor.getAllValues());
        assertEquals(1, set.size());
        assertEquals(ConsistencyLevel.LOCAL_ONE, set.iterator().next());
    }

    @Test
    public void createKeyspaceStatementCQL3Test() throws ParseException
    {
        String expectedFmt = "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = {'class' : '%s', %s}  AND durable_writes = true;\n";
        
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        String expected = String.format(expectedFmt, "keyspace1", SimpleStrategy.class.getName(), "'replication_factor' : '1'" );
        assertEquals( expected, underTest.createKeyspaceStatementCQL3());

        args = new String[] {SettingsSchema.SCHEMA_KEYSPACE.key(), "myKeyspace", SettingsSchema.SCHEMA_REP_STRATEGY.key(), "LocalStrategy", SettingsSchema.SCHEMA_REP_ARGS.key(),  "foo=bar", "fu=baz"};
        commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        expected = String.format(expectedFmt, "myKeyspace", LocalStrategy.class.getName(), "'fu' : 'baz', 'replication_factor' : '1', 'foo' : 'bar'" );
        assertEquals( expected, underTest.createKeyspaceStatementCQL3());
    }

    @Test
    public void createStandard1StatementCQL3Test() throws ParseException
    {
        String expectedFmt = "CREATE TABLE IF NOT EXISTS standard1 (key blob PRIMARY KEY \n"+ ", \"C0\" blob\n" +
                             ", \"C1\" blob\n" +
                             ", \"C2\" blob\n" +
                             ", \"C3\" blob\n" +
                             ", \"C4\" blob) WITH compression = {%s}%s;\n";
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        StressSettingsTest.StressSettingsMockJavaDriver mockedStress = new StressSettingsTest.StressSettingsMockJavaDriver("READ", "-n", "5");

        String expected = String.format(expectedFmt, "", "");
        assertEquals(expected, underTest.createStandard1StatementCQL3(mockedStress));


        args = new String[] {SettingsSchema.SCHEMA_COMPRESSION.key(), "foo", SettingsSchema.SCHEMA_COMPACTION_STRATEGY.key(), "LeveledCompactionStrategy", SettingsSchema.SCHEMA_COMPACTION_ARGS.key(), "foo=bar", "fu=baz"};
        commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        expected = String.format(expectedFmt, "'class' : 'foo'", " AND compaction = {'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'fu' : 'baz', 'foo' : 'bar'}");
        assertEquals(expected, underTest.createStandard1StatementCQL3(mockedStress));

        args = new String[] {SettingsSchema.SCHEMA_COMPRESSION.key(), "foo", SettingsSchema.SCHEMA_COMPACTION_STRATEGY.key(), "LeveledCompactionStrategy"};
        commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        expected = String.format(expectedFmt, "'class' : 'foo'", " AND compaction = {'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}");
        assertEquals(expected, underTest.createStandard1StatementCQL3(mockedStress));
    }

    @Test
    public void createCounter1StatementCQL3Test() throws ParseException
    {
        String expectedFmt = "CREATE TABLE IF NOT EXISTS counter1 (key blob PRIMARY KEY \n"+ ", \"C0\" counter\n" +
                             ", \"C1\" counter\n" +
                             ", \"C2\" counter\n" +
                             ", \"C3\" counter\n" +
                             ", \"C4\" counter) WITH compression = {%s}%s;\n";
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        StressSettingsTest.StressSettingsMockJavaDriver mockedStress = new StressSettingsTest.StressSettingsMockJavaDriver("READ", "-n", "5");

        String expected = String.format(expectedFmt, "", "");
        assertEquals(expected, underTest.createCounter1StatementCQL3(mockedStress));


        args = new String[] {SettingsSchema.SCHEMA_COMPRESSION.key(), "foo", SettingsSchema.SCHEMA_COMPACTION_STRATEGY.key(), "LeveledCompactionStrategy", SettingsSchema.SCHEMA_COMPACTION_ARGS.key(), "foo=bar", "fu=baz"};
        commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        expected = String.format(expectedFmt, "'class' : 'foo'", " AND compaction = {'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'fu' : 'baz', 'foo' : 'bar'}");
        assertEquals(expected, underTest.createCounter1StatementCQL3(mockedStress));

        args = new String[] {SettingsSchema.SCHEMA_COMPRESSION.key(), "foo", SettingsSchema.SCHEMA_COMPACTION_STRATEGY.key(), "LeveledCompactionStrategy"};
        commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        expected = String.format(expectedFmt, "'class' : 'foo'", " AND compaction = {'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}");
        assertEquals(expected, underTest.createCounter1StatementCQL3(mockedStress));
    }

}
