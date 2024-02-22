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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SettingsGraphTest
{

    private Options workingOptions = new Options().addOptions(SettingsCommand.getOptions())
                         .addOptions(SettingsGraph.getOptions());


    @Test
    public void defaultTest() throws ParseException
    {

        String[] args = {"-uncert-err", "-graph-file", "outputFile"};

        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, workingOptions, args);
        CommandLine commandLine = DefaultParser.builder().build().parse(workingOptions, args);

        SettingsGraph underTest = new SettingsGraph(commandLine, settingsCommand);
        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("unknown", underTest.revision);
        assertTrue(underTest.title.startsWith("cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(new Date())));
        assertEquals(Command.HELP.name(), underTest.operation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "File: outputFile");
        logger.assertContains( "Revision: unknown");
        logger.assertContains( "Title: cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(new Date()));
        logger.assertContains( "Operation: HELP");
    }

    @Test
    public void revisionTest() throws ParseException
    {
        String[] args = {"-uncert-err", "-graph-file", "outputFile", "-graph-revision", "revisionString"};

        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, workingOptions, args);
        CommandLine commandLine = DefaultParser.builder().build().parse(workingOptions, args);

        SettingsGraph underTest = new SettingsGraph(commandLine, settingsCommand);
        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("revisionString", underTest.revision);
        assertTrue(underTest.title.startsWith("cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(new Date())));
        assertEquals(Command.HELP.name(), underTest.operation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "File: outputFile");
        logger.assertContains( "Revision: revisionString");
        logger.assertContains( "Title: cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(new Date()));
        logger.assertContains( "Operation: HELP");
    }

    @Test
    public void titleTest() throws ParseException
    {

        String[] args = {"-uncert-err", "-graph-file", "outputFile", "-graph-title", "titleString"};

        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, workingOptions, args);
        CommandLine commandLine = DefaultParser.builder().build().parse(workingOptions, args);

        SettingsGraph underTest = new SettingsGraph(commandLine, settingsCommand);
        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("unknown", underTest.revision);
        assertEquals("titleString", underTest.title);
        assertEquals(Command.HELP.name(), underTest.operation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains( "File: outputFile");
        logger.assertContains( "Revision: unknown");
        logger.assertContains( "Title: titleString");
        logger.assertContains( "Operation: HELP");
    }

    @Test
    public void nameTest() throws ParseException
    {

        String[] args = { "-uncert-err", "-graph-file", "outputFile", "-graph-name", "nameString" };

        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, workingOptions, args);
        CommandLine commandLine = DefaultParser.builder().build().parse(workingOptions, args);

        SettingsGraph underTest = new SettingsGraph(commandLine, settingsCommand);
        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("unknown", underTest.revision);
        assertTrue(underTest.title.startsWith("cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(new Date())));
        assertEquals("nameString", underTest.operation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("File: outputFile");
        logger.assertContains("Revision: unknown");
        logger.assertContains("Title: cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(new Date()));
        logger.assertContains("Operation: nameString");
    }
}
