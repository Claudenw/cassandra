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

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SettingsRateTests
{

    SettingsCommand getSettingsCommand(Command type) throws ParseException
    {
        String[] args = {"-n", "5"};
        return SettingsCommandTests.getInstance(type, SettingsCommand.getOptions(), args);
    }

    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        SettingsRate underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));

        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(0, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 0");
        logger.assertEndsWith("OpsPer Sec: 0");


        // with override forcing command
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.WRITE));
        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(200, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 200");
        logger.assertEndsWith("OpsPer Sec: 0");

    }

    @Test
    public void autoTest() throws ParseException
    {
        String[] args = {"-rate-auto"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        SettingsRate underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));

        assertTrue( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(4 ,underTest.minThreads);
        assertEquals(1000,underTest.maxThreads);
        assertEquals(-1, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: true");
        logger.assertEndsWith("Min Threads: 4");
        logger.assertEndsWith("Max Threads: 1000");

        // with override forcing command
        underTest = underTest = new SettingsRate(commandLine, getSettingsCommand(Command.WRITE));

        assertTrue( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(4 ,underTest.minThreads);
        assertEquals(1000,underTest.maxThreads);
        assertEquals(-1, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: true");
        logger.assertEndsWith("Min Threads: 4");
        logger.assertEndsWith("Max Threads: 1000");

    }

    @Test
    public void fixedTest() throws ParseException
    {
        String[] args = {"-rate-fixed", "5"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        SettingsRate underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));


        assertFalse( underTest.auto);
        assertTrue( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(0, underTest.threadCount);
        assertEquals(5, underTest.opsPerSecond);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 0");
        logger.assertEndsWith("OpsPer Sec: 5");

        // with override forcing command
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.WRITE));

        assertFalse( underTest.auto);
        assertTrue( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(0, underTest.threadCount);
        assertEquals(5, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 0");
        logger.assertEndsWith("OpsPer Sec: 5");

        // test with negative value
        args = new String[] {"-rate-fixed", "-1"};
        commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        try
        {
            underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void conflictingOptionsTest() throws ParseException
    {
        String[] args =  {"-rate-throttle", "3", "-rate-fixed", "5", "-rate-auto"};
        try
        {
            DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing.
        }

        args = new String[] {"-rate-throttle", "3", "-rate-fixed", "5"};
        try
        {
            DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing.
        }


        args = new String[] {"-rate-throttle", "3", "-rate-auto"};
        try
        {
            DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing.
        }

        args = new String[] {"-rate-fixed", "5", "-rate-auto"};
        try
        {
            DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing.
        }
    }

    @Test
    public void minClientTest() throws ParseException
    {

        String[] args = {"-rate-auto", "-rate-min-clients", "10"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        SettingsRate underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));

        assertTrue( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(10 ,underTest.minThreads);
        assertEquals(1000,underTest.maxThreads);
        assertEquals(-1, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: true");
        logger.assertEndsWith("Min Threads: 10");
        logger.assertEndsWith("Max Threads: 1000");

        // with override forcing command
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.WRITE));

        assertTrue( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(10 ,underTest.minThreads);
        assertEquals(1000,underTest.maxThreads);
        assertEquals(-1, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: true");
        logger.assertEndsWith("Min Threads: 10");
        logger.assertEndsWith("Max Threads: 1000");

        // no effect for non-auto
        args = new String[] {"-rate-min-clients", "10"};
        commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));

        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(0, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 0");
        logger.assertEndsWith("OpsPer Sec: 0");

        // test negative value
        try
        {
            args = new String[]{ "-rate-auto", "-rate-min-clients", "-1" };
            commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected)
        {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }
    @Test
    public void maxClientTest() throws ParseException
    {
        String[] args = {"-rate-auto", "-rate-max-clients", "10",};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        SettingsRate underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));


        assertTrue( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(4 ,underTest.minThreads);
        assertEquals(10,underTest.maxThreads);
        assertEquals(-1, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: true");
        logger.assertEndsWith("Min Threads: 4");
        logger.assertEndsWith("Max Threads: 10");

        // with override forcing command
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.WRITE));

        assertTrue( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(4 ,underTest.minThreads);
        assertEquals(10,underTest.maxThreads);
        assertEquals(-1, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: true");
        logger.assertEndsWith("Min Threads: 4");
        logger.assertEndsWith("Max Threads: 10");

        // no effect for non-auto
        args = new String[] {"-rate-min-clients", "10"};
        commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));


        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(0, underTest.threadCount);
        assertEquals(0, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 0");
        logger.assertEndsWith("OpsPer Sec: 0");

        // test negative value
        try
        {
            args = new String[]{ "-rate-auto", "-rate-max-clients", "-1" };
            commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected)
        {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void throttleTest() throws ParseException
    {
        String[] args = {"-rate-throttle", "100" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        SettingsRate underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));


        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(0, underTest.threadCount);
        assertEquals(100, underTest.opsPerSecond);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 0");
        logger.assertEndsWith("OpsPer Sec: 100");


        // with override forcing command
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.WRITE));

        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(0, underTest.threadCount);
        assertEquals(100, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 0");
        logger.assertEndsWith("OpsPer Sec: 100");

        // test with negative value
        args = new String[] {"-rate-throttle", "-1"};
        commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        try
        {
            underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }


    }
    @Test
    public void clientsTest() throws ParseException
    {
        String[] args = {"-rate-throttle", "100", "-rate-clients", "500" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        SettingsRate underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));


        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(500, underTest.threadCount);
        assertEquals(100, underTest.opsPerSecond);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 500");
        logger.assertEndsWith("OpsPer Sec: 100");


        // with override forcing command
        underTest = new SettingsRate(commandLine, getSettingsCommand(Command.WRITE));

        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(500, underTest.threadCount);
        assertEquals(100, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 500");
        logger.assertEndsWith("OpsPer Sec: 100");

        // test with negative value
        args = new String[] {"-rate-throttle", "100", "-rate-clients", "-1" };
        commandLine = DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
        try
        {
            underTest = new SettingsRate(commandLine, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }

    }
}
