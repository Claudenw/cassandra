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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.stress.operations.OpDistributionFactory;
import org.mockito.ArgumentCaptor;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.psjava.util.AssertStatus.assertTrue;

public class SettingsCommandUserTest
{
    @Test
    public void defatulTest() throws ParseException
    {
        String args[] = {};
        try
        {
            DefaultParser.builder().build().parse(SettingsCommandUser.getOptions(), args);
            fail("Should have thrown MissingOptionException");
        } catch (MissingOptionException expected) {
            // do nothing
        }
    }

    static void writeYaml(File tempFile) throws IOException
    {
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            w.write("specname: Test1\n");
            w.write( "keyspace: yaml1keyspace\n");
            w.write( "table: yaml1table\n");
            w.write( "queries : \n" +
                     "    simple1:\n" +
                     "        cql: select * from typestest where name = ? and choice = ? LIMIT 100\n" +
                     "        fields: samerow \n" );
        }
    }

    static SettingsCommandUser getMinimalConfig() throws IOException, ParseException
    {
        File tempFile = FileUtils.createTempFile("cassandra-stress-command-user-test", "yaml");
        writeYaml(tempFile);

        String args[] = { "-n", "5", "-command-profile", tempFile.absolutePath(), "-command-ratio", "insert=2", "simple1=1"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandUser.getOptions(), args);
        return  new SettingsCommandUser(commandLine);
    }

    @Test
    public void minimalTest() throws ParseException, IOException
    {
        SettingsCommandUser underTest = getMinimalConfig();

        assertEquals( 2, underTest.ratios.size());
        assertEquals(2.0, underTest.ratios.get("insert"), 0.000001);
        assertEquals(1.0, underTest.ratios.get("simple1"), 0.000001);
        assertEquals( "Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000", underTest.clustering.getConfigAsString());
        assertFalse(underTest.hasInsertOnly());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContainsRegex("Command Ratios: \\{.*insert=2\\.0");
        logger.assertContainsRegex("Command Ratios: \\{.*simple1=1\\.0");
        logger.assertEndsWith("Command Clustering Distribution: Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000");
    }


    @Test
    public void clusteringTest() throws ParseException, IOException
    {
        File tempFile = FileUtils.createTempFile("cassandra-stress-command-user-test", "yaml");
        writeYaml(tempFile);
        String args[] = { "-n", "5", "-command-profile", tempFile.absolutePath(), "-command-ratio", "insert=2", "simple1=1", "-command-clustering", "FIXED(5)"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandUser.getOptions(), args);
        SettingsCommandUser underTest = new SettingsCommandUser(commandLine);

        assertEquals( 2, underTest.ratios.size());
        assertEquals(2.0, underTest.ratios.get("insert"), 0.000001);
        assertEquals(1.0, underTest.ratios.get("simple1"), 0.000001);
        assertEquals( "Fixed:  key=5", underTest.clustering.getConfigAsString());
        assertFalse(underTest.hasInsertOnly());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContainsRegex("Command Ratios: \\{.*insert=2\\.0");
        logger.assertContainsRegex("Command Ratios: \\{.*simple1=1\\.0");
        logger.assertEndsWith("Command Clustering Distribution: Fixed:  key=5");
    }

    @Test
    public void hasInsertOnlyTest() throws IOException, ParseException
    {
        File tempFile = FileUtils.createTempFile("cassandra-stress-command-user-test", "yaml");
        writeYaml(tempFile);

        String args[] = { "-n", "5", "-command-profile", tempFile.absolutePath(), "-command-ratio", "INSERT=2"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandUser.getOptions(), args);
        SettingsCommandUser underTest = new SettingsCommandUser(commandLine);

        assertEquals( 1, underTest.ratios.size());
        assertEquals(2.0, underTest.ratios.get("INSERT"), 0.000001);
        assertEquals( "Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000", underTest.clustering.getConfigAsString());
        assertTrue(underTest.hasInsertOnly());

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Command Ratios: {INSERT=2.0}");
        logger.assertEndsWith("Command Clustering Distribution: Gaussian:  min=1,max=10,mean=5.500000,stdev=1.500000");
    }

    @Test
    public void truncateTablesTest() throws ParseException, IOException
    {
        List<String> expected = List.of("TRUNCATE yaml1keyspace.yaml1table");
        StressSettingsTest.StressSettingsMockJavaDriver mockedStress = new StressSettingsTest.StressSettingsMockJavaDriver("READ", "-truncate", "always", "-command-ratio", "insert=2", "simple1=1", "-n", "5");

        File tempFile = FileUtils.createTempFile("cassandra-stress-command-user-test", "yaml");
        writeYaml(tempFile);

        String args[] = { "-n", "5", "-command-profile", tempFile.absolutePath(), "-command-ratio", "insert=2", "simple1=1"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandUser.getOptions(), args);
        SettingsCommandUser underTest = new SettingsCommandUser(commandLine);
        underTest.truncateTables(mockedStress);

        ArgumentCaptor<String > cmdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ConsistencyLevel> levelCaptor = ArgumentCaptor.forClass(ConsistencyLevel.class);
        verify(mockedStress.mockDriver, times(1)).execute(cmdCaptor.capture(), levelCaptor.capture());
        assertEquals( expected, cmdCaptor.getAllValues());
        Set<ConsistencyLevel> set = new HashSet<>();
        set.addAll(levelCaptor.getAllValues());
        assertEquals(1, set.size());
        assertEquals(ConsistencyLevel.ONE, set.iterator().next());
    }

    @Test
    public void getFactoryTest() throws ParseException, IOException
    {
        File tempFile = FileUtils.createTempFile("cassandra-stress-command-user-test", "yaml");
        writeYaml(tempFile);

        String args[] = { "-n", "5", "-command-profile", tempFile.absolutePath(), "-command-ratio", "insert=2", "simple1=1"};

        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCommandUser.getOptions(), args);
        SettingsCommandUser underTest = new SettingsCommandUser(commandLine);
        StressSettings stressSettings = new StressSettings("READ","-command-ratio", "insert=2", "simple1=1", "-n", "5");
        OpDistributionFactory factory = underTest.getFactory(stressSettings);
        assertNotNull(factory);
        Assert.assertEquals("[simple1, insert]", factory.desc());
    }
}
