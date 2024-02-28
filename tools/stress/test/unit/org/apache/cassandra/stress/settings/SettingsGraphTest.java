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
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SettingsGraphTest
{
    SettingsCommand getSettingsCommand(Command type, String... args) throws ParseException
    {
        return SettingsCommandTests.getInstance(type, SettingsCommand.getOptions(), args);
    }

    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {"-graph-file", "outputFile"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsGraph.getOptions(), args);
        SettingsGraph underTest = new SettingsGraph(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        Date date = new Date();

        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("unknown", underTest.revision);
        assertTrue(underTest.title.startsWith("cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(date)));
        assertEquals(Command.HELP.name(), underTest.operation);
        assertTrue(underTest.inGraphMode());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("File: outputFile");
        logger.assertEndsWith("Revision: unknown");
        logger.assertContainsRegex("Title: cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm:..").format(date));
        logger.assertEndsWith("Operation: HELP");
    }

    @Test
    public void revisionTest() throws ParseException
    {
        String[] args = {"-graph-file", "outputFile", "-graph-revision", "revisionString"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsGraph.getOptions(), args);
        SettingsGraph underTest = new SettingsGraph(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        Date date = new Date();
        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("revisionString", underTest.revision);
        assertTrue(underTest.title.startsWith("cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(date)));
        assertEquals(Command.HELP.name(), underTest.operation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("File: outputFile");
        logger.assertEndsWith("Revision: revisionString");
        logger.assertContainsRegex("Title: cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm:..").format(date));
        logger.assertEndsWith("Operation: HELP");
    }

    @Test
    public void titleTest() throws ParseException
    {

        String[] args = {"-graph-file", "outputFile", "-graph-title", "titleString"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsGraph.getOptions(), args);
        SettingsGraph underTest = new SettingsGraph(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("unknown", underTest.revision);
        assertEquals("titleString", underTest.title);
        assertEquals(Command.HELP.name(), underTest.operation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("File: outputFile");
        logger.assertEndsWith("Revision: unknown");
        logger.assertEndsWith("Title: titleString");
        logger.assertEndsWith("Operation: HELP");
    }

    @Test
    public void nameTest() throws ParseException
    {

        String[] args = { "-graph-file", "outputFile", "-graph-name", "nameString" };

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsGraph.getOptions(), args);
        SettingsGraph underTest = new SettingsGraph(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        Date date = new Date();
        assertEquals(new File("outputFile"), underTest.file);
        assertEquals("unknown", underTest.revision);
        assertTrue(underTest.title.startsWith("cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm").format(date)));
        assertEquals("nameString", underTest.operation);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("File: outputFile");
        logger.assertEndsWith("Revision: unknown");
        logger.assertContainsRegex("Title: cassandra-stress - " + new SimpleDateFormat("yyyy-mm-dd hh:mm:..").format(date));
        logger.assertEndsWith("Operation: nameString");
    }

    @Test
    public void inGraphModeTest() {
        fail("not implemented");
    }
}
