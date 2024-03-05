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
import org.apache.commons.cli.Options;
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
        String[] args = {SettingsCommand.COUNT.key(), "5"};
        return SettingsCommandTests.getInstance(type, SettingsCommand.getOptions(), args);
    }

    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));

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
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));
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

    }

    @Test
    public void autoTest() throws ParseException
    {
        String[] args = {SettingsRate.RATE_AUTO.key()};
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));

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
        underTest = underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));

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
        String[] args = {SettingsRate.RATE_FIXED.key(), "5"};
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));


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
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));

        assertFalse( underTest.auto);
        assertTrue( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(200, underTest.threadCount);
        assertEquals(5, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 200");
        logger.assertEndsWith("OpsPer Sec: 5");

        // test with negative value
        // have to regen options as they are chagned during parsing.
        args = new String[] {SettingsRate.RATE_FIXED.key(), "-1"};
        options = SettingsRate.getOptions();
        commandLine = DefaultParser.builder().build().parse(options, args);
        try
        {
            underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void conflictingOptionsTest() throws ParseException
    {
        String[] args =  {SettingsRate.RATE_THROTTLE.key(), "3", SettingsRate.RATE_FIXED.key(), "5", SettingsRate.RATE_AUTO.key()};
        try
        {
            DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing.
        }

        args = new String[] {SettingsRate.RATE_THROTTLE.key(), "3", SettingsRate.RATE_FIXED.key(), "5"};
        try
        {
            DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing.
        }


        args = new String[] {SettingsRate.RATE_THROTTLE.key(), "3", SettingsRate.RATE_AUTO.key()};
        try
        {
            DefaultParser.builder().build().parse(SettingsRate.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing.
        }

        args = new String[] {SettingsRate.RATE_FIXED.key(), "5", SettingsRate.RATE_AUTO.key()};
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

        String[] args = {SettingsRate.RATE_AUTO.key(), SettingsRate.RATE_MIN_CLIENTS.key(), "10"};
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));

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
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));

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
        // have to regen options as they are modified by parse
        args = new String[] {SettingsRate.RATE_MIN_CLIENTS.key(), "10"};
        options = SettingsRate.getOptions();
        commandLine = DefaultParser.builder().build().parse(options, args);
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));

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
            args = new String[]{ SettingsRate.RATE_AUTO.key(), SettingsRate.RATE_MIN_CLIENTS.key(), "-1" };
            options = SettingsRate.getOptions();
            commandLine = DefaultParser.builder().build().parse(options, args);
            underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected)
        {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }
    @Test
    public void maxClientTest() throws ParseException
    {
        String[] args = {SettingsRate.RATE_AUTO.key(), SettingsRate.RATE_MAX_CLIENTS.key(), "10",};
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));


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
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));

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
        args = new String[] {SettingsRate.RATE_MIN_CLIENTS.key(), "10"};
        options = SettingsRate.getOptions();
        commandLine = DefaultParser.builder().build().parse(options, args);
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));


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
            args = new String[]{ SettingsRate.RATE_AUTO.key(), SettingsRate.RATE_MAX_CLIENTS.key(), "-1" };
            options = SettingsRate.getOptions();
            commandLine = DefaultParser.builder().build().parse(options, args);
            underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected)
        {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void throttleTest() throws ParseException
    {
        String[] args = {SettingsRate.RATE_THROTTLE.key(), "100" };
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));


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
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));

        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(200, underTest.threadCount);
        assertEquals(100, underTest.opsPerSecond);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Auto: false");
        logger.assertEndsWith("Thread Count: 200");
        logger.assertEndsWith("OpsPer Sec: 100");

        // test with negative value
        args = new String[] {SettingsRate.RATE_THROTTLE.key(), "-1"};
        options = SettingsRate.getOptions();
        commandLine = DefaultParser.builder().build().parse(options, args);
        try
        {
            underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }


    }
    @Test
    public void clientsTest() throws ParseException
    {
        String[] args = {SettingsRate.RATE_THROTTLE.key(), "100", SettingsRate.RATE_THREADS.key(), "500" };
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));

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
        underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));

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
        args = new String[] {SettingsRate.RATE_THROTTLE.key(), "100", SettingsRate.RATE_THREADS.key(), "-1" };
        options = SettingsRate.getOptions();
        commandLine = DefaultParser.builder().build().parse(options, args);
        try
        {
            underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.HELP));
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    /*
        Override only applies with WRITE or COUNTER_WRITE commands where command.count > 0 and
        one of   RATE_AUTO, RATE_FIXED, or RATE_THROTTLE is set.  This test should override the
        -rate-threads 500 with 200.
     */
    @Test
    public void overrideThreadCounterTest() throws ParseException
    {
        String[] args = {SettingsRate.RATE_THROTTLE.key(), "100" };
        Options options = SettingsRate.getOptions();
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        SettingsRate underTest = new SettingsRate(commandLine, options, getSettingsCommand(Command.WRITE));

        assertFalse( underTest.auto);
        assertFalse( underTest.isFixed );
        assertEquals(-1 ,underTest.minThreads);
        assertEquals(-1,underTest.maxThreads);
        assertEquals(200, underTest.threadCount);
        assertEquals(100, underTest.opsPerSecond);
    }
}
