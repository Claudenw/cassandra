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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.stress.report.StressMetrics;
import org.apache.cassandra.stress.util.JavaDriverClient;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class StressSettingsTest
{
    @Test
    public void isSerializable() throws Exception
    {
       // Map<String, String[]> args = new HashMap<>();
       // args.put("write", new String[] {});
        StressSettings settings = new StressSettings( new String[] {"write", "-n", "5"});
        // Will throw if not all settings are Serializable
        new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(settings);
    }

    @Test
    public void test16473()
    {
        Set<String> jmxNodes = StressMetrics.toJmxNodes(new HashSet<String>(Arrays.asList("127.0.0.1:9042", "127.0.0.1")));
        assertEquals(0, jmxNodes.stream().filter(n -> n.contains(":")).count());
    }

    @Test
    public void printSettingsTest() throws Exception
    {
        // Map<String, String[]> args = new HashMap<>();
        // args.put("write", new String[] {});
        StressSettings settings = new StressSettings( new String[] {"write", "-n", "5"});
        TestingResultLogger logger = new TestingResultLogger();
        settings.printSettings(logger);
        // just check for the headings each Settings class verifies its own output.
        logger.assertStartsWith("******************** Stress Settings ********************");
        logger.assertStartsWith("Command:");
        logger.assertStartsWith("Rate:");
        logger.assertStartsWith("Population:");
        logger.assertStartsWith("Insert:");
        logger.assertStartsWith("Columns:");
        logger.assertStartsWith("Errors:");
        logger.assertStartsWith("Log:");
        logger.assertStartsWith("Mode:");
        logger.assertStartsWith("Node:");
        logger.assertStartsWith("Schema:");
        logger.assertStartsWith("Transport:");
        logger.assertStartsWith("Port:");
        logger.assertStartsWith("JMX:");
        logger.assertStartsWith("Graph:");
        logger.assertStartsWith("TokenRange:");
        logger.assertStartsWith("Credentials file:");
        logger.assertStartsWith("Reporting:");
    }

    @Test
    public void printHelpTest() throws Exception
    {
        // Map<String, String[]> args = new HashMap<>();
        // args.put("write", new String[] {});
        StressSettings settings = new StressSettings( new String[] {"write", "--help"});
        TestingResultLogger logger = new TestingResultLogger();
        settings.printHelp();

    }

    public static class StressSettingsMockJavaDriver extends StressSettings
    {
        public JavaDriverClient mockDriver;
        public StressSettingsMockJavaDriver(String... args) throws ParseException
        {
            super(args);
             mockDriver = mock(JavaDriverClient.class);
        }

        @Override
        public JavaDriverClient getJavaDriverClient()
        {
            return mockDriver;
        }

        @Override
        public JavaDriverClient getJavaDriverClient(boolean setKeyspace)
        {
            if (setKeyspace)
                return getJavaDriverClient(schema.keyspace);

            return mockDriver;
        }

        @Override
        public JavaDriverClient getJavaDriverClient(String keyspace)
        {
            if (keyspace != null)
                mockDriver.execute("USE \"" + keyspace + "\";", org.apache.cassandra.db.ConsistencyLevel.ONE);
            return mockDriver;
        }


    }
}
