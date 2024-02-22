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

import com.datastax.driver.core.BatchStatement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SettingsInsertTest
{
    @Test
    public void defaultTest() throws ParseException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsInsert.getOptions(), args);

        SettingsInsert underTest = new SettingsInsert(commandLine);
        assertEquals("Fixed:  key=1", underTest.visits.getConfigAsString());
        assertEquals("Uniform:  min=1,max=1000000", underTest.revisit.getConfigAsString());
        assertNull(underTest.batchsize);
        assertEquals("Ratio: divisor=1.000000;delegate=Fixed:  key=1", underTest.rowPopulationRatio.getConfigAsString());
        assertNull(underTest.selectRatio);
        assertNull(underTest.batchType);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Revisits: Uniform:  min=1,max=1000000");
        logger.assertContains("Visits: Fixed:  key=1");
        logger.assertContains("Row Population Ratio: Ratio: divisor=1.000000;delegate=Fixed:  key=1");
        logger.assertContains("Batch Type: not batching");
    }

    @Test
    public void partitionsTest() throws ParseException
    {
        String[] args = {"-insert-partitions", "FIXED(3)"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsInsert.getOptions(), args);

        SettingsInsert underTest = new SettingsInsert(commandLine);
        assertEquals("Fixed:  key=1", underTest.visits.getConfigAsString());
        assertEquals("Uniform:  min=1,max=1000000", underTest.revisit.getConfigAsString());
        assertEquals("Fixed:  key=3", underTest.batchsize.getConfigAsString());
        assertEquals("Ratio: divisor=1.000000;delegate=Fixed:  key=1", underTest.rowPopulationRatio.getConfigAsString());
        assertNull(underTest.selectRatio);
        assertNull(underTest.batchType);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Revisits: Uniform:  min=1,max=1000000");
        logger.assertContains("Visits: Fixed:  key=1");
        logger.assertContains("Row Population Ratio: Ratio: divisor=1.000000;delegate=Fixed:  key=1");
        logger.assertContains("Batch Type: not batching");
        logger.assertContains("Batchsize: Fixed:  key=3");
    }

    @Test
    public void selectRatioTest() throws ParseException
    {
        String[] args = {"-insert-select-ratio", "FIXED(3)/2"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsInsert.getOptions(), args);

        SettingsInsert underTest = new SettingsInsert(commandLine);
        assertEquals("Fixed:  key=1", underTest.visits.getConfigAsString());
        assertEquals("Uniform:  min=1,max=1000000", underTest.revisit.getConfigAsString());
        assertNull(underTest.batchsize);
        assertEquals("Ratio: divisor=1.000000;delegate=Fixed:  key=1", underTest.rowPopulationRatio.getConfigAsString());
        assertEquals("Ratio: divisor=2.000000;delegate=Fixed:  key=3",underTest.selectRatio.getConfigAsString());
        assertNull(underTest.batchType);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Revisits: Uniform:  min=1,max=1000000");
        logger.assertContains("Visits: Fixed:  key=1");
        logger.assertContains("Row Population Ratio: Ratio: divisor=1.000000;delegate=Fixed:  key=1");
        logger.assertContains("Batch Type: not batching");
        logger.assertContains("Select Ratio: Ratio: divisor=2.000000;delegate=Fixed:  key=3");
    }

    @Test
    public void batchTypeTest() throws ParseException
    {
        String[] args = {"-insert-batchtype", "UNLOGGED" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsInsert.getOptions(), args);

        SettingsInsert underTest = new SettingsInsert(commandLine);
        assertEquals("Fixed:  key=1", underTest.visits.getConfigAsString());
        assertEquals("Uniform:  min=1,max=1000000", underTest.revisit.getConfigAsString());
        assertNull(underTest.batchsize);
        assertEquals("Ratio: divisor=1.000000;delegate=Fixed:  key=1", underTest.rowPopulationRatio.getConfigAsString());
        assertNull(underTest.selectRatio);
        assertEquals(BatchStatement.Type.UNLOGGED, underTest.batchType);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertContains("Revisits: Uniform:  min=1,max=1000000");
        logger.assertContains("Visits: Fixed:  key=1");
        logger.assertContains("Row Population Ratio: Ratio: divisor=1.000000;delegate=Fixed:  key=1");
        logger.assertContains("Batch Type: UNLOGGED");
    }
}
