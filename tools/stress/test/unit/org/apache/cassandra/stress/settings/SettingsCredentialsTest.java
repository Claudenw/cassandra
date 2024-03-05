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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_USERNAME_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_USERNAME_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SettingsCredentialsTest {

    File tempFile = FileUtils.createTempFile("cassandra-stress-credentials-test", "properties");

    static SettingsCredentials getSettingsCredentials(String... args) throws ParseException
    {
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCredentials.getOptions(), args);
        return new SettingsCredentials(commandLine);
    }


    private void writeProperties( Properties properties ) throws IOException
    {
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            properties.store(w, null);
        }
    }

    public static Properties getFullProperties() {
        Properties properties = new Properties();
        properties.setProperty(CQL_USERNAME_PROPERTY_KEY, "cqluserfromfile");
        properties.setProperty(CQL_PASSWORD_PROPERTY_KEY, "cqlpasswordfromfile");
        properties.setProperty(JMX_USERNAME_PROPERTY_KEY, "jmxuserfromfile");
        properties.setProperty(JMX_PASSWORD_PROPERTY_KEY, "jmxpasswordfromfile");
        properties.setProperty(TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY, "keystorepasswordfromfile");
        properties.setProperty(TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY, "truststorepasswordfromfile");
        return properties;
    }


    @Test
    public void testMissinfFileName() throws Exception
    {
        if (tempFile.exists())
        {
            tempFile.delete();
        }

        String[] args = { "-credential-file", tempFile.absolutePath() };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCredentials.getOptions(), args);
        try
        {
            SettingsCredentials underTest = new SettingsCredentials(commandLine);
            fail("Should have thrown FileNotFoundException");
        }
        catch (RuntimeException expected)
        {
            assertEquals(FileNotFoundException.class, expected.getCause().getClass());
        }
    }

    @Test
    public void testNoFileSpecified() throws Exception
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCredentials.getOptions(), args);
        SettingsCredentials underTest = new SettingsCredentials(commandLine);

        assertNull(underTest.cqlUsername);
        assertNull(underTest.cqlPassword);
        assertNull(underTest.jmxUsername);
        assertNull(underTest.jmxPassword);
        assertNull(underTest.transportKeystorePassword);
        assertNull(underTest.transportTruststorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("File: *not set*" );
        logger.assertEndsWith("CQL username: *not set*");
        logger.assertEndsWith("CQL password: *not set*");
        logger.assertEndsWith("JMX username: *not set*");
        logger.assertEndsWith("JMX password: *not set*");
        logger.assertEndsWith("Transport truststore password: *not set*");
        logger.assertEndsWith("Transport keystore password: *not set*");
    }
    @Test
    public void testReadCredentialsFromFileMixed() throws Exception
    {

        writeProperties(getFullProperties());

        String[] args = { "-credential-file", tempFile.absolutePath() };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCredentials.getOptions(), args);
        SettingsCredentials underTest = new SettingsCredentials(commandLine);

        assertEquals("cqluserfromfile", underTest.cqlUsername);
        assertEquals("cqlpasswordfromfile", underTest.cqlPassword);
        assertEquals("jmxuserfromfile", underTest.jmxUsername);
        assertEquals("jmxpasswordfromfile", underTest.jmxPassword);
        assertEquals("keystorepasswordfromfile", underTest.transportKeystorePassword);
        assertEquals("truststorepasswordfromfile", underTest.transportTruststorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("File: " + tempFile.absolutePath() );
        logger.assertEndsWith("CQL username: cqluserfromfile");
        logger.assertEndsWith("CQL password: *suppressed*");
        logger.assertEndsWith("JMX username: jmxuserfromfile");
        logger.assertEndsWith("JMX password: *suppressed*");
        logger.assertEndsWith("Transport truststore password: *suppressed*");
        logger.assertEndsWith("Transport keystore password: *suppressed*");
    }

    @Test
    public void testPartialPropertiesSet() throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty(CQL_USERNAME_PROPERTY_KEY, "cqluserfromfile");
        properties.setProperty(CQL_PASSWORD_PROPERTY_KEY, "cqlpasswordfromfile");
        properties.setProperty(TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY, "keystorestorepasswordfromfile");

        writeProperties(properties);

        String[] args = { "-credential-file", tempFile.absolutePath() };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCredentials.getOptions(), args);
        SettingsCredentials underTest = new SettingsCredentials(commandLine);

        assertEquals("cqluserfromfile", underTest.cqlUsername);
        assertEquals("cqlpasswordfromfile", underTest.cqlPassword);
        assertNull(underTest.jmxUsername);
        assertNull(underTest.jmxPassword);
        assertEquals("keystorestorepasswordfromfile", underTest.transportKeystorePassword);
        assertNull(underTest.transportTruststorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("File: " + tempFile.absolutePath() );
        logger.assertEndsWith("CQL username: cqluserfromfile");
        logger.assertEndsWith("CQL password: *suppressed*");
        logger.assertEndsWith("JMX username: *not set*");
        logger.assertEndsWith("JMX password: *not set*");
        logger.assertEndsWith("Transport truststore password: *not set*");
        logger.assertEndsWith("Transport keystore password: *suppressed*");
    }
}
