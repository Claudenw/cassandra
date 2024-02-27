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
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SettingsCommandPreDefinedTest
{
    @Test
    public void defatulTest() throws ParseException
    {
        String args[] = {};
        try
        {
            DefaultParser.builder().build().parse(SettingsCommandPreDefined.getOptions(), args);
            fail("Should have thrown MissingOptionException");
        } catch (MissingOptionException expected) {
            // do nothing
        }
    }

    @Test
    public void minimalTest() throws ParseException
    {
        String args[] = { "-n", "5"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefined.getOptions(), args);
        SettingsCommandPreDefined underTest = new SettingsCommandPreDefined(Command.HELP, commandLine);

        assertEquals(10, underTest.keySize);
        assertEquals("Fixed:  key=1", underTest.add.getConfigAsString());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith( "Key Size (bytes): 10");
        logger.assertEndsWith( "Counter Increment Distibution: Fixed:  key=1");
    }

    @Test
    public void addTest() throws ParseException
    {
        String args[] = { "-n", "5", "-command-add", "Fixed(5)"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefined.getOptions(), args);
        SettingsCommandPreDefined underTest = new SettingsCommandPreDefined(Command.HELP, commandLine);

        assertEquals(10, underTest.keySize);
        assertEquals("Fixed:  key=5", underTest.add.getConfigAsString());


        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith( "Key Size (bytes): 10");
        logger.assertEndsWith( "Counter Increment Distibution: Fixed:  key=5");
    }

    @Test
    public void keysizeTest() throws ParseException
    {
        String args[] = { "-n", "5", "-command-keysize", "5"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefined.getOptions(), args);
        SettingsCommandPreDefined underTest = new SettingsCommandPreDefined(Command.HELP, commandLine);

        assertEquals(5, underTest.keySize);
        assertEquals("Fixed:  key=1", underTest.add.getConfigAsString());

        String foo = underTest.add.getConfigAsString();


        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith( "Key Size (bytes): 5");
        logger.assertEndsWith( "Counter Increment Distibution: Fixed:  key=1");
    }

    @Test
    public void testTruncateTables() {
        fail("not implemented");
    }

}
