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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SettingsErrorsTests
{
    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsErrors.getOptions(), args);
        SettingsErrors underTest = new SettingsErrors(commandLine);
        assertFalse(underTest.ignore);
        assertEquals(10, underTest.tries);
        assertFalse(underTest.skipReadValidation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Ignore: false");
        logger.assertContains("Tries: 10");
    }

    @Test
    public void skipReadValidationTest() throws ParseException
    {
        String[] args = {"-skip-read-validation"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsErrors.getOptions(), args);
        SettingsErrors underTest = new SettingsErrors(commandLine);
        assertFalse(underTest.ignore);
        assertEquals(10, underTest.tries);
        assertTrue(underTest.skipReadValidation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Ignore: false");
        logger.assertContains("Tries: 10");
        logger.assertContains("Skip validation: true");
    }

    @Test
    public void errorIgnoreTest() throws ParseException
    {
        String[] args = {"-error-ignore"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsErrors.getOptions(), args);
        SettingsErrors underTest = new SettingsErrors(commandLine);
        assertTrue(underTest.ignore);
        assertEquals(10, underTest.tries);
        assertFalse(underTest.skipReadValidation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Ignore: true");
        logger.assertContains("Tries: 10");
        logger.assertContains("Skip validation: false");
    }

    @Test
    public void retriesTest() throws ParseException
    {
        String[] args = {"-retries", "5"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsErrors.getOptions(), args);
        SettingsErrors underTest = new SettingsErrors(commandLine);
        assertFalse(underTest.ignore);
        assertEquals(6, underTest.tries);
        assertFalse(underTest.skipReadValidation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Ignore: false");
        logger.assertContains("Tries: 6");



        args = new String[] {"-retries", "-1"};
        commandLine = DefaultParser.builder().build().parse(SettingsErrors.getOptions(), args);
        try
        {
            new SettingsErrors(commandLine);
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }

    }

}
