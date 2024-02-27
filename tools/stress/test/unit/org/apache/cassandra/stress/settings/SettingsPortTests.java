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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.fail;

public class SettingsPortTests
{
    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPort.getOptions(), args);
        SettingsPort underTest = new SettingsPort(commandLine);
        assertEquals(9042, underTest.nativePort);
        assertEquals(7199, underTest.jmxPort);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Native Port: 9042");
        logger.assertEndsWith("JMX Port: 7199");
    }

    @Test
    public void nativeTest() throws ParseException
    {
        String[] args = {"-port-native", "8080"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPort.getOptions(), args);
        SettingsPort underTest = new SettingsPort(commandLine);
        assertEquals(8080, underTest.nativePort);
        assertEquals(7199, underTest.jmxPort);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Native Port: 8080");
        logger.assertEndsWith("JMX Port: 7199");


        args = new String[] {"-port-native", "65536"};
        commandLine = DefaultParser.builder().build().parse(SettingsPort.getOptions(), args);
        try
        {
            new SettingsPort(commandLine);
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }

        args = new String[] {"-port-native", "-1"};
        commandLine = DefaultParser.builder().build().parse(SettingsPort.getOptions(), args);
        try
        {
            new SettingsPort(commandLine);
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void jmxTest() throws ParseException
    {
        String[] args = {"-port-jmx", "8080"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPort.getOptions(), args);
        SettingsPort underTest = new SettingsPort(commandLine);
        assertEquals(9042, underTest.nativePort);
        assertEquals(8080, underTest.jmxPort);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Native Port: 9042");
        logger.assertEndsWith("JMX Port: 8080");


        args = new String[] {"-port-jmx", "65536"};
        commandLine = DefaultParser.builder().build().parse(SettingsPort.getOptions(), args);
        try
        {
            new SettingsPort(commandLine);
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }

        args = new String[] {"-port-jmx", "-1"};
        commandLine = DefaultParser.builder().build().parse(SettingsPort.getOptions(), args);
        try
        {
            new SettingsPort(commandLine);
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }
}
