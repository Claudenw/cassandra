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


import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.stress.generate.PartitionGenerator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SettingsPopulationTest
{
    SettingsCommand getSettingsCommand(Command type, String... args) throws ParseException
    {
        return SettingsCommandTests.getInstance(type, SettingsCommand.getOptions(), args);
    }

    @Test
    public void defaultsTest() throws ParseException
    {
        // settings command that is a count.
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");

        // settings command that is a count & WRITE
        underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.WRITE, "-n", "5"));
        assertArrayEquals(new long[]{1l, 5l}, underTest.sequence);
        assertNull(underTest.readlookback);
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Sequence: 1..5");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");

        // settings command that is a duration
        underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-duration", "2m"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");

        // settings command that is uncert-err
        underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-uncert-err"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");
    }

    @Test
    public void defaultsTestCommandUser() throws ParseException
    {
        fail("not implemeented");
    }

    @Test
    public void populationOrderTest() throws ParseException
    {
        String[] args = {"-population-order", "SHUFFLED" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.SHUFFLED, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.SHUFFLED);
        logger.assertEndsWith("Wrap: false");

        args = new String[] {"-population-order", "SORTED" };
        commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.SORTED, underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.SORTED);
        logger.assertEndsWith("Wrap: false");

        args = new String[] {"-population-order", "ARBITRARY" };
        commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");
    }

    @Test
    public void populationSequenceTest() throws ParseException
    {
        String[] args = { "-population-seq", "5..10" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));

        assertArrayEquals(new long[]{5l,10l}, underTest.sequence);
        assertNull(underTest.readlookback);
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertTrue(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Sequence: 5..10");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: true");
    }

    @Test
    public void populationDistTest() throws ParseException
    {
        String[] args = {"-population-dist", "FIXED(5)" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Fixed:  key=5",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Fixed:  key=5");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");
    }

    @Test
    public void readLookbackTest() throws ParseException
    {
        // verify without dist-seq it is not applied.
        String[] args = {"-population-read-lookback", "FIXED(5)" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");

        // verify
        args = new String[] {"-population-read-lookback", "FIXED(5)", "-population-seq", "5..10"  };
        commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertArrayEquals( new long[] {5l, 10l}, underTest.sequence);
        assertEquals("Fixed:  key=5", underTest.readlookback.getConfigAsString());
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertTrue(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Sequence: 5..10");
        logger.assertEndsWith("Read Look Back: Fixed:  key=5");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: true");
    }

    @Test
    public void noWrapTest() throws ParseException
    {
        // verify without dist-seq it is not applied.
        String[] args = {"-population-no-wrap" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");

        // verify
        args = new String[] {"-population-no-wrap", "-population-seq", "5..10"  };
        commandLine = DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
        underTest = new SettingsPopulation(commandLine, getSettingsCommand(Command.HELP, "-n", "5"));
        assertArrayEquals( new long[] {5l, 10l}, underTest.sequence);
        assertNull(underTest.readlookback);
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Sequence: 5..10");
        logger.assertEndsWith("Order: " + PartitionGenerator.Order.ARBITRARY);
        logger.assertEndsWith("Wrap: false");
    }

    @Test
    public void distSeqExclusionTest() throws ParseException
    {

        String[] args = { "-population-seq", "1..5", "-population-dist", "FIXED(5)" };
        try
        {
            DefaultParser.builder().build().parse(SettingsPopulation.getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing
        }
    }
}
