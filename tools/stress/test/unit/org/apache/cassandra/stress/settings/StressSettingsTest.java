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
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.stress.report.StressMetrics;
import org.apache.cassandra.stress.util.JavaDriverClient;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_USERNAME_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_USERNAME_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY;
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
        StressSettings settings = new StressSettings( new String[] {"write", "--help"});
        TestingResultLogger logger = new TestingResultLogger();
        settings.printHelp();
    }

    @Test
    public void testReadCredentialsFromFileOverridenByCommandLine() throws Exception
    {
        Properties properties = SettingsCredentialsTest.getFullProperties();

        File tempFile = FileUtils.createTempFile("cassandra-stress-settings-test", "properties");

        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            properties.store(w, null);
        }

        String[] args = {"READ", SettingsCommand.UNCERT_ERR.key(), SettingsMode.MODE_CQL_STYLE.key(), "cql",
                         SettingsMode.MODE_PASSWORD.key(), "cqlpasswordoncommandline",
                         SettingsMode.MODE_USER.key(), "cqluseroncommandline", SettingsJMX.JMX_PASSWORD.key(), "jmxpasswordoncommandline",
                         SettingsJMX.JMX_USER.key(), "jmxuseroncommandline", SettingsTransport.TRANSPORT_TRUSTSTORE.key(), "sometruststore",
                         SettingsTransport.TRANSPORT_KEYSTORE.key(), "somekeystore", SettingsTransport.TRANSPORT_TRUSTSTORE_PASSWORD.key(),
                         "truststorepasswordfromcommandline", SettingsTransport.TRANSPORT_KEYSTORE_PASSWORD.key(), "keystorepasswordfromcommandline",
                         SettingsCredentials.CREDENTIAL_FILE.key(), tempFile.absolutePath()};

        StressSettings settings = new StressSettings(args);
        assertEquals("cqluserfromfile", settings.credentials.cqlUsername);
        assertEquals("cqlpasswordfromfile", settings.credentials.cqlPassword);
        assertEquals("jmxuserfromfile", settings.credentials.jmxUsername);
        assertEquals("jmxpasswordfromfile", settings.credentials.jmxPassword);
        assertEquals("keystorepasswordfromfile", settings.credentials.transportKeystorePassword);
        assertEquals("truststorepasswordfromfile", settings.credentials.transportTruststorePassword);

        assertEquals("cqluseroncommandline", settings.mode.username);
        assertEquals("cqlpasswordoncommandline", settings.mode.password);
        assertEquals("jmxuseroncommandline", settings.jmx.user);
        assertEquals("jmxpasswordoncommandline", settings.jmx.password);
        assertEquals("truststorepasswordfromcommandline", settings.transport.getEncryptionOptions().truststore_password);
        assertEquals("keystorepasswordfromcommandline", settings.transport.getEncryptionOptions().keystore_password);
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
