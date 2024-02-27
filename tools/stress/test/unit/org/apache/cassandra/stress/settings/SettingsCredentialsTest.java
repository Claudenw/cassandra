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
        assertEquals("keystorestorepasswordfromfile", underTest.transportKeystorePassword);
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
    public void testReadCredentialsFromFileOverridenByCommandLine() throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty(CQL_USERNAME_PROPERTY_KEY, "cqluserfromfile");
        properties.setProperty(CQL_PASSWORD_PROPERTY_KEY, "cqlpasswordfromfile");
        properties.setProperty(JMX_USERNAME_PROPERTY_KEY, "jmxuserfromfile");
        properties.setProperty(JMX_PASSWORD_PROPERTY_KEY, "jmxpasswordfromfile");
        properties.setProperty(TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY, "keystorestorepasswordfromfile");
        properties.setProperty(TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY, "truststorepasswordfromfile");

        File tempFile = FileUtils.createTempFile("cassandra-stress-credentials-test", "properties");

        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            properties.store(w, null);
        }

        Map<String, String[]> args = new HashMap<>();
        args.put("write", new String[]{});
        args.put("-mode", new String[]{ "cql3", "native", "password=cqlpasswordoncommandline", "user=cqluseroncommandline" });
        args.put("-jmx", new String[]{ "password=jmxpasswordoncommandline", "user=jmxuseroncommandline" });
        args.put("-transport", new String[]{ "truststore=sometruststore",
                                             "keystore=somekeystore",
                                             "truststore-password=truststorepasswordfromcommandline",
                                             "keystore-password=keystorepasswordfromcommandline" });
        args.put("-credentials-file", new String[]{ tempFile.absolutePath() });
        StressSettings settings = StressSettings.get(args);

        assertEquals("cqluserfromfile", settings.credentials.cqlUsername);
        assertEquals("cqlpasswordfromfile", settings.credentials.cqlPassword);
        assertEquals("jmxuserfromfile", settings.credentials.jmxUsername);
        assertEquals("jmxpasswordfromfile", settings.credentials.jmxPassword);
        assertEquals("keystorestorepasswordfromfile", settings.credentials.transportKeystorePassword);
        assertEquals("truststorepasswordfromfile", settings.credentials.transportTruststorePassword);

        assertEquals("cqluseroncommandline", settings.mode.username);
        assertEquals("cqlpasswordoncommandline", settings.mode.password);
        assertEquals("jmxuseroncommandline", settings.jmx.user);
        assertEquals("jmxpasswordoncommandline", settings.jmx.password);
        assertEquals("truststorepasswordfromcommandline", settings.transport.getEncryptionOptions().truststore_password);
        assertEquals("keystorepasswordfromcommandline", settings.transport.getEncryptionOptions().keystore_password);
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
