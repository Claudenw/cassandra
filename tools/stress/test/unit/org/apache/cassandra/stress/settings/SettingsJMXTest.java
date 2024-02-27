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


import java.io.IOException;
import java.io.Writer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SettingsJMXTest
{
    SettingsCredentials getSettingsCredentials(String... args) throws ParseException
    {
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCredentials.getOptions(), args);
        return new SettingsCredentials(commandLine);
    }

    @Test
    public void defaultTest() throws ParseException, IOException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsJMX.getOptions(), args);
        SettingsJMX underTest = new SettingsJMX(commandLine, getSettingsCredentials());
        assertNull(underTest.password);
        assertNull(underTest.user);
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *not set*");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-jmx-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsJMX(commandLine, getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertEquals("jmxpasswordfromfile", underTest.password);
        assertEquals("jmxuserfromfile", underTest.user);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: jmxuserfromfile");
        logger.assertEndsWith("Password: *suppressed*");
    }

    @Test
    public void userTest() throws ParseException, IOException
    {
        String[] args = { "-jmx-user", "commandlineuser"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsJMX.getOptions(), args);
        SettingsJMX underTest = new SettingsJMX(commandLine, getSettingsCredentials());
        assertNull(underTest.password);
        assertEquals("commandlineuser", underTest.user);
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: commandlineuser");
        logger.assertEndsWith("Password: *not set*");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-jmx-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsJMX(commandLine, getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertEquals("jmxpasswordfromfile", underTest.password);
        assertEquals("commandlineuser", underTest.user);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: commandlineuser");
        logger.assertEndsWith("Password: *suppressed*");
    }

    @Test
    public void passwordTest() throws ParseException, IOException
    {
        String[] args = { "-jmx-password", "commandlinepassword"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsJMX.getOptions(), args);
        SettingsJMX underTest = new SettingsJMX(commandLine, getSettingsCredentials());
        assertEquals("commandlinepassword", underTest.password);
        assertNull(underTest.user);
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *suppressed*");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-jmx-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsJMX(commandLine, getSettingsCredentials( "-credential-file", tempFile.absolutePath()));
        assertEquals("commandlinepassword", underTest.password);
        assertEquals("jmxuserfromfile", underTest.user);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: jmxuserfromfile");
        logger.assertEndsWith("Password: *suppressed*");
    }
}
