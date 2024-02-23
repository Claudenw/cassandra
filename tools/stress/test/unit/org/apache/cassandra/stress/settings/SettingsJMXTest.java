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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SettingsJMXTest
{
    private Options workingOptions = new Options().addOptions(SettingsJMX.getOptions()).addOptions(SettingsCredentials.getOptions());

    @Test
    public void defaultTest() throws ParseException, IOException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(workingOptions, args);
        SettingsCredentials settingsCredentials = new SettingsCredentials(commandLine);
        SettingsJMX underTest = new SettingsJMX(commandLine, settingsCredentials);
        assertNull(underTest.password);
        assertNull(underTest.user);
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "Username: *not set*");
        logger.assertContains( "Password: *not set*");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-jmx-test", "properties");
        args = new String[] { "-credential-file", tempFile.absolutePath()};
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        commandLine = DefaultParser.builder().build().parse(workingOptions, args);
        settingsCredentials = new SettingsCredentials(commandLine);
        underTest = new SettingsJMX(commandLine, settingsCredentials);
        assertEquals("jmxpasswordfromfile", underTest.password);
        assertEquals("jmxuserfromfile", underTest.user);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "Username: jmxuserfromfile");
        logger.assertContains( "Password: *suppressed*");
    }

    @Test
    public void userTest() throws ParseException, IOException
    {
        String[] args = { "-jmx-user", "commandlineuser"};
        CommandLine commandLine = DefaultParser.builder().build().parse(workingOptions, args);
        SettingsCredentials settingsCredentials = new SettingsCredentials(commandLine);
        SettingsJMX underTest = new SettingsJMX(commandLine, settingsCredentials);
        assertNull(underTest.password);
        assertEquals("commandlineuser", underTest.user);
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "Username: commandlineuser");
        logger.assertContains( "Password: *not set*");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-jmx-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        args = new String[] { "-credential-file", tempFile.absolutePath(), "-jmx-user", "commandlineuser"};
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        commandLine = DefaultParser.builder().build().parse(workingOptions, args);
        settingsCredentials = new SettingsCredentials(commandLine);
        underTest = new SettingsJMX(commandLine, settingsCredentials);
        assertEquals("jmxpasswordfromfile", underTest.password);
        assertEquals("commandlineuser", underTest.user);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "Username: commandlineuser");
        logger.assertContains( "Password: *suppressed*");
    }

    @Test
    public void passwordTest() throws ParseException, IOException
    {
        String[] args = { "-jmx-password", "commandlinepassword"};
        CommandLine commandLine = DefaultParser.builder().build().parse(workingOptions, args);
        SettingsCredentials settingsCredentials = new SettingsCredentials(commandLine);
        SettingsJMX underTest = new SettingsJMX(commandLine, settingsCredentials);
        assertEquals("commandlinepassword", underTest.password);
        assertNull(underTest.user);
        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "Username: *not set*");
        logger.assertContains( "Password: *suppressed*");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-jmx-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        args = new String[] { "-credential-file", tempFile.absolutePath(), "-jmx-password", "commandlinepassword"};
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        commandLine = DefaultParser.builder().build().parse(workingOptions, args);
        settingsCredentials = new SettingsCredentials(commandLine);
        underTest = new SettingsJMX(commandLine, settingsCredentials);
        assertEquals("commandlinepassword", underTest.password);
        assertEquals("jmxuserfromfile", underTest.user);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "Username: jmxuserfromfile");
        logger.assertContains( "Password: *suppressed*");
    }
}
