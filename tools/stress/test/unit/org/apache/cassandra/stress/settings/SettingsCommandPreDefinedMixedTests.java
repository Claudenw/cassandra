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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;


import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class SettingsCommandPreDefinedMixedTests
{


    @Test
    public void defatulTest() throws ParseException
    {
        String args[] = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        SettingsCommandPreDefinedMixed underTest = new SettingsCommandPreDefinedMixed(commandLine);
        assertEquals( 2, underTest.ratios.size());
        assertEquals(1.0, underTest.ratios.get(Command.READ), 0.000001);
        assertEquals(1.0, underTest.ratios.get(Command.WRITE), 0.000001);
        assertEquals( "Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000", underTest.clustering.getConfigAsString());
    }

    @Test
    public void minimalTest() throws ParseException
    {
        String args[] = { "-n", "5"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        SettingsCommandPreDefinedMixed underTest = new SettingsCommandPreDefinedMixed(commandLine);

        assertEquals( 2, underTest.ratios.size());
        assertEquals(1.0, underTest.ratios.get(Command.READ), 0.000001);
        assertEquals(1.0, underTest.ratios.get(Command.WRITE), 0.000001);
        assertEquals( "Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000", underTest.clustering.getConfigAsString());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContainsRegex("Command Ratios: \\{.*READ=1\\.0");
        logger.assertContainsRegex("Command Ratios: \\{.*WRITE=1\\.0");
        logger.assertEndsWith("Command Clustering Distribution: Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000");
    }

    @Test
    public void ratioTest() throws ParseException
    {
        String args[] = { "-n", "5", "-command-ratio", "read=4.3", "write=1.2"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        SettingsCommandPreDefinedMixed underTest = new SettingsCommandPreDefinedMixed(commandLine);

        assertEquals( 2, underTest.ratios.size());
        assertEquals(4.3, underTest.ratios.get(Command.READ), 0.000001);
        assertEquals(1.2, underTest.ratios.get(Command.WRITE), 0.000001);
        assertEquals( "Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000", underTest.clustering.getConfigAsString());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContainsRegex("Command Ratios: \\{.*READ=4\\.3");
        logger.assertContainsRegex("Command Ratios: \\{.*WRITE=1\\.2");
        logger.assertEndsWith("Command Clustering Distribution: Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000");
    }

    @Test
    public void clusteringTest() throws ParseException
    {
        String args[] = { "-n", "5", "-command-clustering", "FIXED(5)"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        SettingsCommandPreDefinedMixed underTest = new SettingsCommandPreDefinedMixed(commandLine);

        assertEquals( 2, underTest.ratios.size());
        assertEquals(1.0, underTest.ratios.get(Command.READ), 0.000001);
        assertEquals(1.0, underTest.ratios.get(Command.WRITE), 0.000001);
        assertEquals( "Fixed:  key=5", underTest.clustering.getConfigAsString());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContainsRegex("Command Ratios: \\{.*READ=1\\.0");
        logger.assertContainsRegex("Command Ratios: \\{.*WRITE=1\\.0");
        logger.assertEndsWith("Command Clustering Distribution: Fixed:  key=5");
    }

    @Test
    public void getFactoryTest() throws ParseException, IOException
    {
        String args[] = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        SettingsCommand underTest =  new SettingsCommandPreDefinedMixed(commandLine);
        StressSettings stressSettings = new StressSettings("READ", "-n", "5");
        OpDistributionFactory factory = underTest.getFactory(stressSettings);
        assertNotNull(factory);
        assertEquals("[WRITE, READ]", factory.desc());
    }

    @Test
    public void truncateTablesTest() throws ParseException, IOException
    {
        List<String> expected = List.of("TRUNCATE keyspace1.standard1", "TRUNCATE keyspace1.counter1", "TRUNCATE keyspace1.counter3");
        StressSettingsTest.StressSettingsMockJavaDriver mockedStress = new StressSettingsTest.StressSettingsMockJavaDriver("READ", "-truncate", "always", "-n", "5");

        String args[] = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
        SettingsCommand underTest =  new SettingsCommandPreDefinedMixed(commandLine);
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

}
