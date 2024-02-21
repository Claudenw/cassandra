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

import java.util.List;
import java.util.ArrayList;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.stress.util.ResultLogger;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SettingsColumnTests
{

    @Test
    public void defaultParsingTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertEquals(5, underTest.maxColumnsPerKey);
        assertEquals( underTest.maxColumnsPerKey, underTest.names.size());
        assertEquals("C0", underTest.namestrs.get(0).toString());
        assertEquals("C1", underTest.namestrs.get(1).toString());
        assertEquals("C2", underTest.namestrs.get(2).toString());
        assertEquals("C3", underTest.namestrs.get(3).toString());
        assertEquals("C4", underTest.namestrs.get(4).toString());
        assertNull(underTest.comparator);
        assertNull(underTest.timestamp);
        assertFalse(underTest.variableColumnCount);
        assertFalse(underTest.slice);
        assertEquals("Fixed:  key=34", underTest.sizeDistribution.getConfigAsString());
        assertEquals("Fixed:  key=5", underTest.countDistribution.getConfigAsString());
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Max Columns Per Key: 5");
        logger.assertContains("Column Names: [C0, C1, C2, C3, C4]");
        logger.assertContains("Comparator: null");
        logger.assertContains("Timestamp: null");
        logger.assertContains("Variable Column Count: false");
        logger.assertContains("Slice: false");
        logger.assertContains("Size Distribution: Fixed:  key=34");
        logger.assertContains("Count Distribution: Fixed:  key=5");
    }

    @Test
    public void parsingWithNamesTest() throws ParseException
    {
        String[] args = { "-col-names", "name1,name2,name3", "-col-size", "FIXED(10)",  "-col-slice", "-col-timestamp", "1708441057"};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertEquals(3, underTest.maxColumnsPerKey);
        assertEquals( 3, underTest.names.size());
        assertEquals("name1", underTest.namestrs.get(0).toString());
        assertEquals("name2", underTest.namestrs.get(1).toString());
        assertEquals("name3", underTest.namestrs.get(2).toString());
        assertEquals(AsciiType.instance.getClass(), underTest.comparator.getClass());
        assertEquals("1708441057", underTest.timestamp);
        assertFalse(underTest.variableColumnCount);
        assertTrue(underTest.slice);
        assertEquals("Fixed:  key=10", underTest.sizeDistribution.getConfigAsString());
        assertEquals("Count:  fixed=3", underTest.countDistribution.getConfigAsString());
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "Max Columns Per Key: 3");
        logger.assertContains( "Column Names: [name1, name2, name3]");
        logger.assertContains("Comparator: "+AsciiType.instance.getClass().getName());
        logger.assertContains("Timestamp: 1708441057");
        logger.assertContains("Variable Column Count: false");
        logger.assertContains("Slice: true");
        logger.assertContains("Size Distribution: Fixed:  key=10");
        logger.assertContains("Count Distribution: Count:  fixed=3");

        System.out.println(commandLine);
    }

    @Test
    public void comparatorTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertNull(underTest.comparator);

        args = new String[] { "-col-comparator", "AsciiType"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertNull(underTest.comparator);

        // test default type
        args = new String[] { "-col-names", "one,two" };
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals(AsciiType.instance.getClass(), underTest.comparator.getClass());


        // test explicit setting
        args = new String[] { "-col-names", "one,two", "-col-comparator", "AsciiType"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals(AsciiType.instance.getClass(), underTest.comparator.getClass());

        args = new String[] { "-col-names", "58e0a7d7-eebc-11d8-9669-0800200c9a66", "-col-comparator", "TimeUUIDType"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals(TimeUUIDType.instance.getClass(), underTest.comparator.getClass());

        args = new String[] { "-col-names", "one,two", "-col-comparator", "UTF8Type"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals(UTF8Type.instance.getClass(), underTest.comparator.getClass());

        // test bad value
        args = new String[] { "-col-names", "one,two", "-col-comparator", "BadName"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        try
        {
            new SettingsColumn(commandLine);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ignore.
        }
    }

    @Test
    public void sliceTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertFalse(underTest.slice);

        args = new String [] {"-col-slice"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertTrue(underTest.slice);
    }

    @Test
    public void timestampTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertNull(underTest.timestamp);

        args = new String [] {"-col-timestamp", "1708441057"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals("1708441057", underTest.timestamp);
    }

    @Test
    public void namesTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertEquals(List.of("C0", "C1", "C2", "C3", "C4"), underTest.namestrs);

        args = new String [] {"-col-names", "a,b,c"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals(List.of("a","b","c"), underTest.namestrs);
    }

    @Test
    public void sizeTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertEquals("org.apache.cassandra.stress.settings.OptionDistribution$FixedFactory", underTest.sizeDistribution.getClass().getName());
        assertEquals("Fixed:  key=34", underTest.sizeDistribution.getConfigAsString());

        args = new String [] {"-col-size", "FIXED(7)"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals("org.apache.cassandra.stress.settings.OptionDistribution$FixedFactory", underTest.sizeDistribution.getClass().getName());
        assertEquals("Fixed:  key=7", underTest.sizeDistribution.getConfigAsString());
    }

    @Test
    public void countTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        SettingsColumn underTest = new SettingsColumn(commandLine);
        assertEquals("org.apache.cassandra.stress.settings.OptionDistribution$FixedFactory", underTest.countDistribution.getClass().getName());
        assertEquals("Fixed:  key=5", underTest.countDistribution.getConfigAsString());

        args = new String [] {"-col-count", "FIXED(7)"};
        commandLine = DefaultParser.builder().build().parse( SettingsColumn.getOptions(), args);
        underTest = new SettingsColumn(commandLine);
        assertEquals("org.apache.cassandra.stress.settings.OptionDistribution$FixedFactory", underTest.countDistribution.getClass().getName());
        assertEquals("Fixed:  key=7", underTest.countDistribution.getConfigAsString());
    }

    @Test
    public void countAndNamesTest() throws ParseException
    {
        String[] args = {"-col-count", "FIXED(7)", "-col-names", "0ne,two,three"};
        try
        {
            CommandLine commandLine = DefaultParser.builder().build().parse(SettingsColumn.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        }
        catch (AlreadySelectedException expected) {
            // do nothing.
        }
    }

    class TestingResultLogger implements ResultLogger
    {
        List<String> results = new ArrayList<>();

        public void assertContains(String s) {
            assertTrue(String.format("Missing '%s'", s), results.stream().filter( str -> str.contains(s) ).findFirst().isPresent());
        }
        @Override
        public void println(String line)
        {
            results.add(line);
        }

        @Override
        public void println()
        {
        }

        @Override
        public void printException(Exception e)
        {
            println(e.toString());
        }

        @Override
        public void flush()
        {

        }

        @Override
        public void printf(String s, Object... args)
        {
            println(String.format(s, args));
        }
    }

}
