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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        String[] args = {"-schema-replication", "LocalStrategy"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertTrue(underTest.compactionStrategyOptions.isEmpty());
        assertNull(underTest.compression);
        assertEquals(org.apache.cassandra.locator.LocalStrategy.class, underTest.replicationStrategy);
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
        String[] args = {"-schema-replication-args", "foo=bar", "fu=baz"};
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
        String[] args = {"-schema-compaction", "LeveledCompactionStrategy"};
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
        String[] args = {"-schema-compaction-args", "foo=bar", "fu=baz"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsSchema.getOptions(), args);
        SettingsSchema underTest = new SettingsSchema(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals( "keyspace1", underTest.keyspace);
        assertNull(underTest.compactionStrategy);
        assertEquals(org.apache.cassandra.locator.SimpleStrategy.class, underTest.replicationStrategy);
        assertEquals(2, underTest.compactionStrategyOptions.size());
        assertEquals("bar", underTest.compactionStrategyOptions.get("foo"));
        assertEquals("baz", underTest.compactionStrategyOptions.get("fu"));
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
        logger.assertEndsWith("Table Compaction Strategy: null");
        logger.assertContainsRegex("Table Compaction Strategy Options:.+'fu' : 'baz'");
        logger.assertContainsRegex("Table Compaction Strategy Options:.+'foo' : 'bar'");
    }

    @Test
    public void keyspaceTest() throws ParseException
    {
        String[] args = {"-schema-keyspace", "ks1"};
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
        String[] args = {"-schema-compression", "foo"};
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
    public void schemaOptionsWithUserCommandTest() {
        fail("not implemented");
    }

    @Test
    public void createrKeyspacesTest() {
        fail("not implemented");
    }

    @Test
    public void createrKeyspaceStatementCQL3Test() {
        fail("not implemented");
    }

    @Test
    public void createrStandard1StatementCQL3Test() {
        fail("not implemented");
    }

    @Test
    public void createrCounter1StatementCQL3Test() {
        fail("not implemented");
    }

}
