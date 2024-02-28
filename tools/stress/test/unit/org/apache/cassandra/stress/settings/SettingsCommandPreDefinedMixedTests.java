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

public class SettingsCommandPreDefinedMixedTests
{
    @Test
    public void defatulTest() throws ParseException
    {
        String args[] = {};
        try
        {
            DefaultParser.builder().build().parse(SettingsCommandPreDefinedMixed.getOptions(), args);
            fail("Should have thrown MissingOptionException");
        } catch (MissingOptionException expected) {
            // do nothing
        }
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
    public void getFactoryTest() {
        fail("not implemented");
    }

}
