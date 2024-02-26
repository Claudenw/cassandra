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

import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SettingsReportingTests
{
    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsReporting.getOptions(), args);
        SettingsReporting underTest = new SettingsReporting(commandLine);
        assertEquals(0, underTest.headerFrequency.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.headerFrequency.unit());
        assertEquals(1, underTest.outputFrequency.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.outputFrequency.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Output frequency: 1s");
        logger.assertContains("Header frequency: 0s");
    }

    @Test
    public void headerFreqTest() throws ParseException
    {
        String[] args = {"-reporting-header-freq", "3m"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsReporting.getOptions(), args);
        SettingsReporting underTest = new SettingsReporting(commandLine);
        assertEquals(3, underTest.headerFrequency.quantity());
        assertEquals(TimeUnit.MINUTES, underTest.headerFrequency.unit());
        assertEquals(1, underTest.outputFrequency.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.outputFrequency.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Output frequency: 1s");
        logger.assertContains("Header frequency: 3m");
    }

    @Test
    public void outputFreqTest() throws ParseException
    {
        String[] args = {"-reporting-output-freq", "3s"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsReporting.getOptions(), args);
        SettingsReporting underTest = new SettingsReporting(commandLine);
        assertEquals(0, underTest.headerFrequency.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.headerFrequency.unit());
        assertEquals(3, underTest.outputFrequency.quantity());
        assertEquals(TimeUnit.SECONDS, underTest.outputFrequency.unit());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Output frequency: 3s");
        logger.assertContains("Header frequency: 0s");
    }
}
