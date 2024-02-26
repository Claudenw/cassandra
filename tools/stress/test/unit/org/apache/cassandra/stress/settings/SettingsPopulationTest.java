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
import org.apache.commons.cli.Options;
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
    Options getOptions() {
        return new Options().addOptions(SettingsCommand.getOptions()).addOptions(SettingsPopulation.getOptions());
    }
    @Test
    public void defaultsTest() throws ParseException
    {
        // settings command that is a count.
        String[] args = {"-n", "5"};
        CommandLine commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");

        // settings command that is a count & WRITE
        commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        settingsCommand = SettingsCommandTests.getInstance(Command.WRITE, getOptions(), args);
        underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertArrayEquals(new long[]{1l, 5l}, underTest.sequence);
        assertNull(underTest.readlookback);
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Sequence: 1..5");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");

        // settings command that is a duration
        args = new String[] {"-duration", "2m"};
        commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");

        // settings command that is uncert-err
        args = new String[] {"-uncert-err"};
        commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY,underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=1000000,mean=500000.500000,stdev=166666.500000");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");
    }

    @Test
    public void defaultsTestCommandUser() throws ParseException
    {
        fail("not implemeented");
    }

    @Test
    public void populationOrderTest() throws ParseException
    {
        String[] args = {"-n", "5", "-population-order", "SHUFFLED" };
        CommandLine commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.SHUFFLED, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertContains("Order: "+PartitionGenerator.Order.SHUFFLED);
        logger.assertContains("Wrap: false");

        args = new String[] {"-n", "5", "-population-order", "SORTED" };
        commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.SORTED, underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertContains("Order: "+PartitionGenerator.Order.SORTED);
        logger.assertContains("Wrap: false");

        args = new String[] {"-n", "5", "-population-order", "ARBITRARY" };
        commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");
    }

    @Test
    public void populationSequenceTest() throws ParseException
    {
        String[] args = { "-population-seq", "5..10" };
        CommandLine commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertArrayEquals(new long[]{5l,10l}, underTest.sequence);
        assertNull(underTest.readlookback);
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertTrue(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Sequence: 5..10");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: true");
    }

    @Test
    public void populationDistTest() throws ParseException
    {
        String[] args = {"-n", "5", "-population-dist", "FIXED(5)" };
        CommandLine commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Fixed:  key=5",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Fixed:  key=5");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");
    }
    /*
                                                                                           .build());
    public static final StressOption<DistributionFactory> POPULATION_READ = new StressOption<>(Option.builder("population-read-lookback").hasArg()
                                                                                                     .desc(format("Select read seeds from the recently visited write seeds. Only applies if -%s is specified.", POPULATION_SEQ.key()))
                                                                                                     .type(DistributionFactory.class).build());
    public static final StressOption<String> POPULATION_NO_WRAP = new StressOption<>(Option.builder("population-no-wrap")

     */

    @Test
    public void readLookbackTest() throws ParseException
    {
        // verify without dist-seq it is not applied.
        String[] args = {"-n", "5", "-population-read-lookback", "FIXED(5)" };
        CommandLine commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");

        // verify
        args = new String[] {"-n", "5", "-population-read-lookback", "FIXED(5)", "-population-seq", "5..10"  };
        commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertArrayEquals( new long[] {5l, 10l}, underTest.sequence);
        assertEquals("Fixed:  key=5", underTest.readlookback.getConfigAsString());
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertTrue(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Sequence: 5..10");
        logger.assertContains("Read Look Back: Fixed:  key=5");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: true");
    }

    @Test
    public void noWrapTest() throws ParseException
    {
        // verify without dist-seq it is not applied.
        String[] args = {"-n", "5", "-population-no-wrap" };
        CommandLine commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        SettingsCommand settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        SettingsPopulation underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertNull(underTest.sequence);
        assertNull(underTest.readlookback);
        assertEquals("Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667",underTest.distribution.getConfigAsString());
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Distribution: Gaussian:  min=1,max=5,mean=3.000000,stdev=0.666667");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");

        // verify
        args = new String[] {"-n", "5", "-population-no-wrap", "-population-seq", "5..10"  };
        commandLine = DefaultParser.builder().build().parse(getOptions(), args);
        settingsCommand = SettingsCommandTests.getInstance(Command.HELP, getOptions(), args);
        underTest = new SettingsPopulation(commandLine, settingsCommand);
        assertArrayEquals( new long[] {5l, 10l}, underTest.sequence);
        assertNull(underTest.readlookback);
        assertNull(underTest.distribution);
        assertEquals(PartitionGenerator.Order.ARBITRARY, underTest.order);
        assertFalse(underTest.wrap);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Sequence: 5..10");
        logger.assertContains("Order: "+PartitionGenerator.Order.ARBITRARY);
        logger.assertContains("Wrap: false");
    }

    @Test
    public void distSeqExclusionTest() throws ParseException
    {

        String[] args = { "-population-seq", "1..5", "-population-dist", "FIXED(5)" };
        try
        {
            DefaultParser.builder().build().parse(getOptions(), args);
            fail("Should have thrown AlreadySelectedException");
        } catch (AlreadySelectedException expected) {
            // do nothing
        }
    }
}
