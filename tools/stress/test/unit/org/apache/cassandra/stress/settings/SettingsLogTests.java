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

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SettingsLogTests
{
    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        SettingsLog underTest = new SettingsLog(commandLine);
        assertFalse(underTest.noSettings);
        assertFalse(underTest.noSummary);
        assertEquals(SettingsLog.Level.NORMAL, underTest.level);
        assertNull(underTest.file);
        assertNull(underTest.hdrFile);
        assertEquals(1, underTest.interval.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.interval.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("No Summary: false");
        logger.assertEndsWith("No Settings: false");
        logger.assertEndsWith("File: null");
        logger.assertEndsWith("Interval: 1s");
        logger.assertEndsWith("Level: NORMAL");
    }

    @Test
    public void noSummaryTest() throws ParseException
    {
        String[] args = { "-log-no-summary" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        SettingsLog underTest = new SettingsLog(commandLine);
        assertFalse(underTest.noSettings);
        assertTrue(underTest.noSummary);
        assertEquals(SettingsLog.Level.NORMAL, underTest.level);
        assertNull(underTest.file);
        assertNull(underTest.hdrFile);
        assertEquals(1, underTest.interval.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.interval.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("No Summary: true");
        logger.assertEndsWith("No Settings: false");
        logger.assertEndsWith("File: null");
        logger.assertEndsWith("Interval: 1s");
        logger.assertEndsWith("Level: NORMAL");
    }

    @Test
    public void noSettingsTest() throws ParseException
    {
        String[] args = { "-log-no-settings" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        SettingsLog underTest = new SettingsLog(commandLine);
        assertTrue(underTest.noSettings);
        assertFalse(underTest.noSummary);
        assertEquals(SettingsLog.Level.NORMAL, underTest.level);
        assertNull(underTest.file);
        assertNull(underTest.hdrFile);
        assertEquals(1, underTest.interval.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.interval.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("No Summary: false");
        logger.assertEndsWith("No Settings: true");
        logger.assertEndsWith("File: null");
        logger.assertEndsWith("Interval: 1s");
        logger.assertEndsWith("Level: NORMAL");
    }

    @Test
    public void fileTest() throws ParseException
    {
        String[] args = { "-log-file", "logFile" };
        File expected = new File("logFile");
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        SettingsLog underTest = new SettingsLog(commandLine);
        assertFalse(underTest.noSettings);
        assertFalse(underTest.noSummary);
        assertEquals(SettingsLog.Level.NORMAL, underTest.level);
        assertEquals(expected, underTest.file);
        assertNull(underTest.hdrFile);
        assertEquals(1, underTest.interval.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.interval.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("No Summary: false");
        logger.assertEndsWith("No Settings: false");
        logger.assertEndsWith("File: " + expected);
        logger.assertEndsWith("Interval: 1s");
        logger.assertEndsWith("Level: NORMAL");
    }

    @Test
    public void headerFileTest() throws ParseException
    {
        String[] args = { "-log-header-file", "headerFile" };
        File expected = new File("headerFile");
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        SettingsLog underTest = new SettingsLog(commandLine);
        assertFalse(underTest.noSettings);
        assertFalse(underTest.noSummary);
        assertEquals(SettingsLog.Level.NORMAL, underTest.level);
        assertNull(underTest.file);
        assertEquals(expected, underTest.hdrFile);
        assertEquals(1, underTest.interval.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.interval.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("No Summary: false");
        logger.assertEndsWith("No Settings: false");
        logger.assertEndsWith("File: null");
        logger.assertEndsWith("Interval: 1s");
        logger.assertEndsWith("Level: NORMAL");
    }

    @Test
    public void logIntervalTest() throws ParseException
    {
        String[] args = { "-log-interval", "3m" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        SettingsLog underTest = new SettingsLog(commandLine);
        assertFalse(underTest.noSettings);
        assertFalse(underTest.noSummary);
        assertEquals(SettingsLog.Level.NORMAL, underTest.level);
        assertNull(underTest.file);
        assertNull(underTest.hdrFile);
        assertEquals(3, underTest.interval.quantity());
        assertEquals(TimeUnit.MINUTES, underTest.interval.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("No Summary: false");
        logger.assertEndsWith("No Settings: false");
        logger.assertEndsWith("File: null");
        logger.assertEndsWith("Interval: 3m");
        logger.assertEndsWith("Level: NORMAL");
    }

    @Test
    public void loglevelTest() throws ParseException
    {
        String[] args = { "-log-level", "VERBOSE" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        SettingsLog underTest = new SettingsLog(commandLine);
        assertFalse(underTest.noSettings);
        assertFalse(underTest.noSummary);
        assertEquals(SettingsLog.Level.VERBOSE, underTest.level);
        assertNull(underTest.file);
        assertNull(underTest.hdrFile);
        assertEquals(1, underTest.interval.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.interval.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("No Summary: false");
        logger.assertEndsWith("No Settings: false");
        logger.assertEndsWith("File: null");
        logger.assertEndsWith("Interval: 1s");

        args = new String[]{ "-log-level", "MINIMAL" };
        commandLine = DefaultParser.builder().build().parse(SettingsLog.getOptions(), args);
        underTest = new SettingsLog(commandLine);
        assertFalse(underTest.noSettings);
        assertFalse(underTest.noSummary);
        assertEquals(SettingsLog.Level.MINIMAL, underTest.level);
        assertNull(underTest.file);
        assertNull(underTest.hdrFile);
        assertEquals(1, underTest.interval.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.interval.unit());
    }
}
