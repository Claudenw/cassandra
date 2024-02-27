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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

public class SettingsTokentRangeTests
{

    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTokenRange.getOptions(), args);
        SettingsTokenRange underTest = new SettingsTokenRange(commandLine);
        assertFalse(underTest.wrap);
        assertEquals(1, underTest.splitFactor);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Wrap: false");
        logger.assertEndsWith("Split Factor: 1");
    }

    @Test
    public void wrapTest() throws ParseException
    {
        String[] args = {"-token-range-wrap"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTokenRange.getOptions(), args);
        SettingsTokenRange underTest = new SettingsTokenRange(commandLine);
        assertTrue(underTest.wrap);
        assertEquals(1, underTest.splitFactor);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Wrap: true");
        logger.assertEndsWith("Split Factor: 1");
    }

    @Test
    public void splitTest() throws ParseException
    {
        String[] args = {"-token-range-split", "5"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTokenRange.getOptions(), args);
        SettingsTokenRange underTest = new SettingsTokenRange(commandLine);
        assertFalse(underTest.wrap);
        assertEquals(5, underTest.splitFactor);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Wrap: false");
        logger.assertEndsWith("Split Factor: 5");

        args = new String[] {"-token-range-split", "-1"};
        commandLine = DefaultParser.builder().build().parse(SettingsTokenRange.getOptions(), args);
        try
        {
            new SettingsTokenRange(commandLine);
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }

    }
}
