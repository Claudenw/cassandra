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
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SettingsTransportTests
{

    public SettingsCredentials getSettingsCredentials(String... args) throws ParseException
    {
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsCredentials.getOptions(), args);
        return new SettingsCredentials(commandLine);
    }

    @Test
    public void defaultTest() throws ParseException, IOException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertNull(underTest.truststore);
        assertNull(underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertNull(underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *not set*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *not set*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }

        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-transport-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsTransport(commandLine, getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertNull(underTest.truststore);
        assertEquals("truststorepasswordfromfile", underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertEquals("keystorepasswordfromfile", underTest.keystorePassword);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *suppressed*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *suppressed*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }
    }

    @Test
    public void truststoreTest() throws ParseException, IOException
    {
        String[] args = { "-ssl-truststore", "atruststore" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertEquals("atruststore", underTest.truststore);
        assertNull(underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertNull(underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: atruststore");
        logger.assertEndsWith("Truststore Password: *not set*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *not set*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }

        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-transport-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsTransport(commandLine, getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertEquals("atruststore", underTest.truststore);
        assertEquals("truststorepasswordfromfile", underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertEquals("keystorepasswordfromfile", underTest.keystorePassword);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: atruststore");
        logger.assertEndsWith("Truststore Password: *suppressed*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *suppressed*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }
    }

    @Test
    public void truststorePasswordTest() throws ParseException, IOException
    {
        String[] args = { "-ssl-truststore-password", "atruststorepassword" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertNull(underTest.truststore);
        assertEquals("atruststorepassword", underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertNull(underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *suppressed*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *not set*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }

        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-transport-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsTransport(commandLine, getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertNull(underTest.truststore);
        assertEquals("atruststorepassword", underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertEquals("keystorepasswordfromfile", underTest.keystorePassword);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *suppressed*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *suppressed*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }
    }

    @Test
    public void keystoreTest() throws ParseException, IOException
    {
        String[] args = { "-ssl-keystore", "akeystore" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertNull(underTest.truststore);
        assertNull(underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertEquals("akeystore", underTest.keystore);
        assertNull(underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *not set*");
        logger.assertEndsWith("Keystore: akeystore");
        logger.assertEndsWith("Keystore Password: *not set*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }

        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-transport-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsTransport(commandLine, getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertNull(underTest.truststore);
        assertEquals("truststorepasswordfromfile", underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertEquals("akeystore", underTest.keystore);
        assertEquals("keystorepasswordfromfile", underTest.keystorePassword);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *suppressed*");
        logger.assertEndsWith("Keystore: akeystore");
        logger.assertEndsWith("Keystore Password: *suppressed*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }
    }

    @Test
    public void keystorePasswordTest() throws ParseException, IOException
    {
        String[] args = { "-ssl-keystore-password", "akeystorepassword" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertNull(underTest.truststore);
        assertNull(underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertEquals("akeystorepassword", underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *not set*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *suppressed*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }

        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-transport-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsTransport(commandLine, getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertNull(underTest.truststore);
        assertEquals("truststorepasswordfromfile", underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertEquals("akeystorepassword", underTest.keystorePassword);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *suppressed*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *suppressed*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }
    }

    @Test
    public void protocolTest() throws ParseException, IOException
    {
        String[] args = { "-ssl-protocol", "PROTOCOL" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertNull(underTest.truststore);
        assertNull(underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("PROTOCOL", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertNull(underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *not set*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *not set*");
        logger.assertEndsWith("SSL Protocol: PROTOCOL");
        logger.assertEndsWith("SSL Algorithm: null");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }
    }

    @Test
    public void algorithmTest() throws ParseException, IOException
    {
        String[] args = { "-ssl-algorithm", "algorithm" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertNull(underTest.truststore);
        assertNull(underTest.truststorePassword);
        assertEquals("algorithm", underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT, underTest.ciphers);
        assertNull(underTest.keystore);
        assertNull(underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *not set*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *not set*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: algorithm");
        for (String s : SettingsTransport.TRANSPORT_CIPHERS_DEFAULT)
        {
            logger.assertContainsRegex(String.format("SSL Ciphers:.*%s", s));
        }
    }

    @Test
    public void ciphersTest() throws ParseException, IOException
    {
        String[] args = { "-ssl-ciphers", "cipher1", "cipher2" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());
        assertNull(underTest.truststore);
        assertNull(underTest.truststorePassword);
        assertNull(underTest.algorithm);
        assertEquals("TLS", underTest.protocol);
        assertArrayEquals(new String[]{ "cipher1", "cipher2" }, underTest.ciphers);
        assertNull(underTest.keystore);
        assertNull(underTest.keystorePassword);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);

        logger.assertEndsWith("Truststore: *not set*");
        logger.assertEndsWith("Truststore Password: *not set*");
        logger.assertEndsWith("Keystore: *not set*");
        logger.assertEndsWith("Keystore Password: *not set*");
        logger.assertEndsWith("SSL Protocol: TLS");
        logger.assertEndsWith("SSL Algorithm: null");
        logger.assertEndsWith("SSL Ciphers: cipher1,cipher2");
    }

    @Test
    public void encryptionOptionsTest() throws ParseException
    {

        EncryptionOptions base = new EncryptionOptions().applyConfig();

        // default does not change
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals(base.truststore, underTest.encryptionOptions.truststore);
        assertEquals(base.truststore_password, underTest.encryptionOptions.truststore_password);

        assertEquals(base.keystore, underTest.encryptionOptions.keystore);
        assertEquals(base.keystore_password, underTest.encryptionOptions.keystore_password);

        assertEquals(base.algorithm, underTest.encryptionOptions.algorithm);
        assertEquals(base.cipher_suites, underTest.encryptionOptions.cipher_suites);
        assertEquals(base.getProtocol(), underTest.encryptionOptions.getProtocol());

        // without truststore all other options are ignored.

        args = new String[]{ "-ssl-truststore-password", "trustpassword", "-ssl-keystore", "akeystore", "-ssl-keystore-password", "keypassword", "-ssl-protocol", "FOO", "-ssl-algorithm", "BAR", "-ssl-ciphers", "cipher1", "cipher2" };
        commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals(base.truststore, underTest.encryptionOptions.truststore);
        assertEquals(base.truststore_password, underTest.encryptionOptions.truststore_password);

        assertEquals(base.keystore, underTest.encryptionOptions.keystore);
        assertEquals(base.keystore_password, underTest.encryptionOptions.keystore_password);

        assertEquals(base.algorithm, underTest.encryptionOptions.algorithm);
        assertEquals(base.cipher_suites, underTest.encryptionOptions.cipher_suites);
        assertEquals(base.getProtocol(), underTest.encryptionOptions.getProtocol());

        // truststore changes things
        args = new String[]{ "-ssl-truststore", "atruststore", "-ssl-truststore-password", "trustpassword", "-ssl-keystore", "akeystore", "-ssl-keystore-password", "keypassword", "-ssl-protocol", "FOO", "-ssl-algorithm", "BAR", "-ssl-ciphers", "cipher1", "cipher2" };
        commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals("atruststore", underTest.encryptionOptions.truststore);
        assertEquals("trustpassword", underTest.encryptionOptions.truststore_password);

        assertEquals("akeystore", underTest.encryptionOptions.keystore);
        assertEquals("keypassword", underTest.encryptionOptions.keystore_password);

        assertEquals("BAR", underTest.encryptionOptions.algorithm);
        assertEquals(List.of("cipher1", "cipher2"), underTest.encryptionOptions.cipher_suites);
        assertEquals("FOO", underTest.encryptionOptions.getProtocol());
    }

    @Test
    public void encryptionOptionsWithoutKeystoreTest() throws ParseException
    {

        EncryptionOptions base = new EncryptionOptions().applyConfig();

        // truststore sets keystore
        String[] args = { "-ssl-truststore", "atruststore", "-ssl-truststore-password", "trustpassword", "keypassword", "-ssl-protocol", "FOO", "-ssl-algorithm", "BAR", "-ssl-ciphers", "cipher1", "cipher2" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals("atruststore", underTest.encryptionOptions.truststore);
        assertEquals("trustpassword", underTest.encryptionOptions.truststore_password);

        assertEquals("atruststore", underTest.encryptionOptions.keystore);
        assertEquals("trustpassword", underTest.encryptionOptions.keystore_password);

        assertEquals("BAR", underTest.encryptionOptions.algorithm);
        assertEquals(List.of("cipher1", "cipher2"), underTest.encryptionOptions.cipher_suites);
        assertEquals("FOO", underTest.encryptionOptions.getProtocol());

        // truststore sets keystore when keystore-passord is set but keystore is not

        args = new String[]{ "-ssl-truststore", "atruststore", "-ssl-truststore-password", "trustpassword", "-ssl-keystore-password", "keypassword", "-ssl-protocol", "FOO", "-ssl-algorithm", "BAR", "-ssl-ciphers", "cipher1", "cipher2" };
        commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals("atruststore", underTest.encryptionOptions.truststore);
        assertEquals("trustpassword", underTest.encryptionOptions.truststore_password);

        assertEquals("atruststore", underTest.encryptionOptions.keystore);
        assertEquals("trustpassword", underTest.encryptionOptions.keystore_password);

        assertEquals("BAR", underTest.encryptionOptions.algorithm);
        assertEquals(List.of("cipher1", "cipher2"), underTest.encryptionOptions.cipher_suites);
        assertEquals("FOO", underTest.encryptionOptions.getProtocol());
    }

    @Test
    public void encryptionOptionsWithoutProtocol() throws ParseException
    {

        EncryptionOptions base = new EncryptionOptions().applyConfig();

        // truststore sets keystore
        String[] args = { "-ssl-truststore", "atruststore", "-ssl-truststore-password", "trustpassword", "keypassword", "-ssl-algorithm", "BAR", "-ssl-ciphers", "cipher1", "cipher2" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals("atruststore", underTest.encryptionOptions.truststore);
        assertEquals("trustpassword", underTest.encryptionOptions.truststore_password);

        assertEquals("atruststore", underTest.encryptionOptions.keystore);
        assertEquals("trustpassword", underTest.encryptionOptions.keystore_password);

        assertEquals("BAR", underTest.encryptionOptions.algorithm);
        assertEquals(List.of("cipher1", "cipher2"), underTest.encryptionOptions.cipher_suites);
        assertEquals("TLS", underTest.encryptionOptions.getProtocol());
    }

    @Test
    public void encryptionOptionsWithoutAlgorithm() throws ParseException
    {

        EncryptionOptions base = new EncryptionOptions().applyConfig();

        // truststore sets keystore
        String[] args = { "-ssl-truststore", "atruststore", "-ssl-truststore-password", "trustpassword", "keypassword", "-ssl-protocol", "FOO", "-ssl-ciphers", "cipher1", "cipher2" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals("atruststore", underTest.encryptionOptions.truststore);
        assertEquals("trustpassword", underTest.encryptionOptions.truststore_password);

        assertEquals("atruststore", underTest.encryptionOptions.keystore);
        assertEquals("trustpassword", underTest.encryptionOptions.keystore_password);

        assertEquals(base.algorithm, underTest.encryptionOptions.algorithm);
        assertEquals(List.of("cipher1", "cipher2"), underTest.encryptionOptions.cipher_suites);
        assertEquals("FOO", underTest.encryptionOptions.getProtocol());
    }

    @Test
    public void encryptionOptionsWithoutCiphers() throws ParseException
    {

        EncryptionOptions base = new EncryptionOptions().applyConfig();

        // truststore sets keystore
        String[] args = { "-ssl-truststore", "atruststore", "-ssl-truststore-password", "trustpassword", "keypassword", "-ssl-protocol", "FOO", "-ssl-algorithm", "BAR" };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsTransport.getOptions(), args);
        SettingsTransport underTest = new SettingsTransport(commandLine, getSettingsCredentials());

        assertEquals("atruststore", underTest.encryptionOptions.truststore);
        assertEquals("trustpassword", underTest.encryptionOptions.truststore_password);

        assertEquals("atruststore", underTest.encryptionOptions.keystore);
        assertEquals("trustpassword", underTest.encryptionOptions.keystore_password);

        assertEquals("BAR", underTest.encryptionOptions.algorithm);
        assertEquals(List.of(SettingsTransport.TRANSPORT_CIPHERS_DEFAULT), underTest.encryptionOptions.cipher_suites);
        assertEquals("FOO", underTest.encryptionOptions.getProtocol());
    }
}

