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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class SettingsCommandPreDefinedTest
{

    @Test
    public void getFactoryTest() throws ParseException, IOException
    {
        String args[] = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefined.getOptions(), args);
        SettingsCommand underTest = new SettingsCommandPreDefined(Command.READ, commandLine);
        StressSettings stressSettings = new StressSettings("READ");
        OpDistributionFactory factory = underTest.getFactory(stressSettings);
        assertNotNull(factory);
        assertEquals("READ", factory.desc());
    }

    @Test
    public final void truncateTablesTest() throws ParseException, IOException
    {
        List<String> expected = List.of("TRUNCATE keyspace1.standard1", "TRUNCATE keyspace1.counter1", "TRUNCATE keyspace1.counter3");
        StressSettingsTest.StressSettingsMockJavaDriver mockedStress = new StressSettingsTest.StressSettingsMockJavaDriver("READ", "-truncate", "always");

        String args[] = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefined.getOptions(), args);
        SettingsCommand underTest = new SettingsCommandPreDefined(Command.READ, commandLine);
        underTest.truncateTables(mockedStress);

        ArgumentCaptor<String > cmdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ConsistencyLevel> levelCaptor = ArgumentCaptor.forClass(ConsistencyLevel.class);
        verify(mockedStress.mockDriver, times(3)).execute(cmdCaptor.capture(), levelCaptor.capture());
        assertEquals( expected, cmdCaptor.getAllValues());
        Set<ConsistencyLevel> set = new HashSet<>();
        set.addAll(levelCaptor.getAllValues());
        assertEquals(1, set.size());
        assertEquals(ConsistencyLevel.ONE, set.iterator().next());
    }
    @Test
    public void defaultTest() throws ParseException
    {
        String args[] = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefined.getOptions(), args);
        SettingsCommandPreDefined underTest = new SettingsCommandPreDefined(Command.HELP, commandLine);
        assertEquals(10, underTest.keySize);
        assertEquals("Fixed:  key=1", underTest.add.getConfigAsString());
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

}
