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
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.stress.operations.OpDistributionFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SettingsCommandTests
{

    static SettingsCommand getInstance(Command type, Options options, String[] args) throws ParseException
    {
        CommandLine commandLine = DefaultParser.builder().build().parse(options, args);
        return new SettingsCommand(type, commandLine) {
            @Override
            public OpDistributionFactory getFactory(StressSettings settings)
            {
                return null;
            }

            @Override
            public void truncateTables(StressSettings settings)
            {

            }
        };
    }

    /*

    public static final StressOption<String> NO_WARMUP = new StressOption<>(new Option("no-warmup", "Do not warmup the process"));
    public static final StressOption<DurationSpec.IntSecondsBound> DURATION = new StressOption<>(Option.builder("duration").hasArg().type(DurationSpec.IntSecondsBound.class).desc("Time to run.").required(true).build());
    public static final StressOption<Long> COUNT = new StressOption<>(Option.builder("n").hasArg().desc("Number of operations to perform. Number may be followd by 'm' or 'k' (e.g. 5m).").type(Long.class).converter(DISTRIBUTION_CONVERTER).required(true).build());

    public static final StressOption<Double> UNCERT_ERR = new StressOption<>(()->0.2d, rangeVerifier(0.0, Range.exclusive, 1.0, Range.exclusive), Option.builder("uncert-err").hasArg()
                                                                                                                                                        .desc("Run until the standard error of the mean is below this fraction. (Default 0.2)").type(Double.class).build());
    public static final StressOption<Integer> UNCERT_MIN = new StressOption<>(()->30, POSITIVE_VERIFIER, Option.builder("uncert-min").hasArg().desc(format("Run at least this many iterations before accepting uncertainty convergence. Only valid with %s. (Default 30)", UNCERT_ERR.key()))
                                                                                                               .type(Integer.class).build());
    public static final StressOption<Integer> UNCERT_MAX = new StressOption<>(()->200, POSITIVE_VERIFIER, Option.builder("uncert-max").hasArg().desc(format("Run at least this many iterations before accepting uncertainty convergence. Only valid with %s. (Default 200)", UNCERT_ERR.key()))
                                                                                                                .type(Integer.class).build());
    public static final StressOption<ConsistencyLevel> CONSISTENCY = new StressOption<>(()->ConsistencyLevel.LOCAL_ONE, Option.builder("cl").hasArg().desc(format("Consistency level to use. Valid options are %s. (Default %s)", enumOptionString(ConsistencyLevel.LOCAL_ONE), ConsistencyLevel.LOCAL_ONE))
                                                                                                                              .type(ConsistencyLevel.class).converter(ConsistencyLevel::valueOf).build());

    /* other options
    public static final StressOption<String> COMMAND_PROFILE = new StressOption<>(Option.builder("command-profile").desc("Specify the path to a yaml cql3 profile. Multiple comma separated files can be added.").hasArgs().required().build());
    public static final StressOption<String> COMMAND_RATIO = new StressOption<>(Option.builder("command-ratio").hasArgs().desc("Specify the ratios for operations to perform. (e.g. (read=2 write=1) will perform 2 reads for each write)").build());
    public static final StressOption<Integer> COMMAND_KEYSIZE = new StressOption<>(()->10, POSITIVE_VERIFIER, Option.builder("comamnd-keysize").hasArg().desc("Key size in bytes. (Default 10)").type(Integer.class).build());
    public static final StressOption<TruncateWhen> TRUNCATE = new StressOption<>(()->TruncateWhen.NEVER, Option.builder("truncate").hasArg().desc(format("When to truncate the table Valid values are %s. (Default %s)", enumOptionString(TruncateWhen.NEVER), TruncateWhen.NEVER))
                                                                                                               .converter(TruncateWhen::valueOf).build());
    private static final String COMMAND_CLUSTERING_DEFAULT = "GAUSSIAN(1..10)";
    public static final StressOption<DistributionFactory> COMMAND_CLUSTERING = new StressOption<>(()->OptionDistribution.get(COMMAND_CLUSTERING_DEFAULT), Option.builder("command-clustering").hasArg().desc(format("Distribution clustering runs of operations of the same kind. (Default %s)", COMMAND_CLUSTERING_DEFAULT))
                                                                                                                                                                .type(DistributionFactory.class).build());
    private static final String COMMAND_ADD_DEFAULT = "FIXED(1)";
    public static final StressOption<DistributionFactory> COMMAND_ADD = new StressOption<>(()->OptionDistribution.get(COMMAND_ADD_DEFAULT), Option.builder("command-add").hasArg().type(DistributionFactory.class).desc(format("Distribution of value of counter increments. (Default %s)", COMMAND_ADD_DEFAULT)).build());

     */

    @Test
    public void defaultTest() throws ParseException
    {
        // try default HELP command
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull(underTest.duration);
        assertEquals(-1L, underTest.count);
        assertEquals( 0.2, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);
    }

    @Test
    public void durationTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-duration", "2m"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertEquals( 2, underTest.duration.quantity());
        assertEquals(TimeUnit.MINUTES, underTest.duration.unit());
        assertEquals(-1L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);
    }

    @Test
    public void countTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "0"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(0L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        try {
            getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "-1"});
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected)
        {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void uncertErrorTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "0.7" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.7d, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        try {
            getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-1",});
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected)
        {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }

    }

    @Test
    public void uncertMinTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-uncert-min", "50" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 50, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-uncert-min", "0" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 0, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        try
        {
            underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-uncert-min", "-1" });
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void uncertMaxTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-uncert-max", "500" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 500, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-uncert-max", "0" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 0, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        try
        {
            underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-uncert-max", "-1" });
            fail("Should have thrown ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
        }

    }

    @Test
    public void noWarmupTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-duration", "2m", "-no-warmup"});
        assertEquals(Command.HELP, underTest.type);
        assertTrue(underTest.noWarmup);
        assertEquals( 2, underTest.duration.quantity());
        assertEquals(TimeUnit.MINUTES, underTest.duration.unit());
        assertEquals(-1L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-no-warmup" });
        assertEquals(Command.HELP, underTest.type);
        assertTrue(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-no-warmup"});
        assertEquals(Command.HELP, underTest.type);
        assertTrue(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

    }

    @Test
    public void truncateTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-duration", "2m", "-truncate", "ONCE" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertEquals( 2, underTest.duration.quantity());
        assertEquals(TimeUnit.MINUTES, underTest.duration.unit());
        assertEquals(-1L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.ONCE, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-truncate", "ALWAYS" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.ALWAYS, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-truncate", "NEVER"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);
    }

    @Test
    public void consistencyTest() throws ParseException
    {
        SettingsCommand underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-duration", "2m", "-cl", "ONE" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertEquals( 2, underTest.duration.quantity());
        assertEquals(TimeUnit.MINUTES, underTest.duration.unit());
        assertEquals(-1L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-uncert-err", "-cl", "ALL" });
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals( 0.2d, underTest.targetUncertainty, 0.00001);
        assertEquals( 30, underTest.minimumUncertaintyMeasurements);
        assertEquals( 200, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.ALL, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "ANY"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.ANY, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "QUORUM"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.QUORUM, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "LOCAL_ONE"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_ONE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "SERIAL"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.SERIAL, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "LOCAL_QUORUM"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_QUORUM, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "EACH_QUORUM"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.EACH_QUORUM, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "LOCAL_SERIAL"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "NODE_LOCAL"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.NODE_LOCAL, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "TWO"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.TWO, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);

        underTest = getInstance(Command.HELP, SettingsCommand.getOptions(), new String[]{ "-n", "5", "-cl", "THREE"});
        assertEquals(Command.HELP, underTest.type);
        assertFalse(underTest.noWarmup);
        assertNull( underTest.duration);
        assertEquals(5L, underTest.count);
        assertEquals( -1.0, underTest.targetUncertainty, 0.00001);
        assertEquals( -1, underTest.minimumUncertaintyMeasurements);
        assertEquals( -1, underTest.maximumUncertaintyMeasurements);
        assertEquals(ConsistencyLevel.THREE, underTest.consistencyLevel);
        assertEquals(SettingsCommand.TruncateWhen.NEVER, underTest.truncate);
    }

    @Test
    public void getTest() throws ParseException, IOException
    {
        /* options an command line have to be paird for this test.  if any values are parsed then we need to retrieve the options again */
        Options options = SettingsCommand.getOptions();
        CommandLine emptyCommandLine = DefaultParser.builder().build().parse(options, new String[]{});

        // HELP commands alwasy return null.
        assertNull(SettingsCommand.get(Command.HELP, emptyCommandLine, options));
        assertNull(SettingsCommand.get(Command.PRINT, emptyCommandLine, options));
        assertNull(SettingsCommand.get(Command.VERSION, emptyCommandLine, options));

        // test BASIC command
        try {
            SettingsCommand.get(Command.READ, emptyCommandLine, options );
            fail("Should have thrown MissingOptionException");
        } catch (RuntimeException e) {
            assertEquals(MissingOptionException.class, e.getCause().getClass());
        }
        CommandLine commandLine = DefaultParser.builder().build().parse(options, new String[] {"-n", "4"});
        assertEquals(SettingsCommandPreDefined.class, SettingsCommand.get(Command.READ, commandLine, options ).getClass());


        // test MIXED command
        try {
            options = SettingsCommand.getOptions();
            emptyCommandLine = DefaultParser.builder().build().parse(options, new String[]{});
            SettingsCommand.get(Command.MIXED, emptyCommandLine, options );
            fail("Should have thrown MissingOptionException");
        } catch (RuntimeException e) {
            assertEquals(MissingOptionException.class, e.getCause().getClass());
        }
        commandLine = DefaultParser.builder().build().parse(options, new String[] {"-n", "4"});
        assertEquals(SettingsCommandPreDefinedMixed.class, SettingsCommand.get(Command.MIXED, commandLine, options ).getClass());

        // test USER command
        File tempFile = FileUtils.createTempFile("cassandra-stress-command-user-test", "yaml");
        SettingsCommandUserTest.writeYaml(tempFile);

        try {
            options = SettingsCommandUser.getOptions();
            emptyCommandLine = DefaultParser.builder().build().parse(options, new String[]{"-command-profile", tempFile.absolutePath(), "-command-ratio", "insert=2", "simple1=1"});
            SettingsCommand.get(Command.USER, emptyCommandLine, options );
            fail("Should have thrown MissingOptionException");
        } catch (RuntimeException e) {
            assertEquals(MissingOptionException.class, e.getCause().getClass());
        }

        String args[] = { "-n", "5", "-command-profile", tempFile.absolutePath(), "-command-ratio", "insert=2", "simple1=1"};
        commandLine = DefaultParser.builder().build().parse(options, args);
        assertEquals(SettingsCommandUser.class, SettingsCommand.get(Command.USER, commandLine, options ).getClass());

    }
}
