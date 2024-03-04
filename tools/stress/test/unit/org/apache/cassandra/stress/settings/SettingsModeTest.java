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
import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.exceptions.AuthenticationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SettingsModeTest
{

    @Test
    public void defaultTest() throws ParseException, IOException
    {
        String[] args = {};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertNull(underTest.password);
        assertNull(underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *not set*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-mode-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertEquals("cqlpasswordfromfile", underTest.password);
        assertEquals("cqluserfromfile", underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: cqluserfromfile");
        logger.assertEndsWith("Password: *suppressed*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");
    }

    @Test
    public void userTest() throws ParseException, IOException
    {
        String[] args = { "-user", "modeuser"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertNull(underTest.password);
        assertEquals("modeuser", underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: modeuser");
        logger.assertEndsWith("Password: *not set*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-mode-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertEquals("cqlpasswordfromfile", underTest.password);
        assertEquals("modeuser", underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: modeuser");
        logger.assertEndsWith("Password: *suppressed*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");
    }

    @Test
    public void passwordTest() throws ParseException, IOException
    {
        String[] args = {"-password", "modepassword"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertEquals("modepassword", underTest.password);
        assertNull(underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *suppressed*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");


        // try with configuration file
        File tempFile = FileUtils.createTempFile("cassandra-stress-mode-test", "properties");
        try (Writer w = tempFile.newWriter(OVERWRITE))
        {
            SettingsCredentialsTest.getFullProperties().store(w, null);
        }
        underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials("-credential-file", tempFile.absolutePath()));
        assertEquals("modepassword", underTest.password);
        assertEquals("cqluserfromfile", underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: cqluserfromfile");
        logger.assertEndsWith("Password: *suppressed*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");
    }

    @Test
    public void protocolVersionTest() throws ParseException
    {
        String[] args = { "-protocol-version", "1" };
        for (ProtocolVersion ver : ProtocolVersion.values())
        {
            args[1] = String.format("%s", ver.toInt());
            CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
            SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
            assertNull(ver.name(), underTest.password);
            assertNull(ver.name(), underTest.username);
            assertNull(ver.name(), underTest.authProvider);
            assertEquals(ver.name(), ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
            assertEquals(ver.name(), Integer.valueOf(8), underTest.connectionsPerHost);
            assertEquals(ver.name(), Integer.valueOf(128), underTest.maxPendingPerConnection);
            assertEquals(ver.name(), ver, underTest.protocolVersion);
            assertEquals(ver.name(), ConnectionStyle.CQL_PREPARED, underTest.style);
            assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

            TestingResultLogger logger = new TestingResultLogger();
            underTest.printSettings(logger);
            logger.assertEndsWith("Username: *not set*", ()->ver.name());
            logger.assertEndsWith("Password: *not set*", ()->ver.name());
            logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE, ()->ver.name());
            logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED, ()->ver.name());
            logger.assertEndsWith("Protocol Version: " + ver, ()->ver.name());
            logger.assertEndsWith("Auth Provider Class: *not set*", ()->ver.name());
            logger.assertEndsWith("Max Pending Per Connection: 128", ()->ver.name());
            logger.assertEndsWith("Connections Per Host: 8", ()->ver.name());
            logger.assertEndsWith("Compression: NONE", ()->ver.name());
        }
    }

    @Test
    public void cqlStyleTest() throws ParseException
    {
        String[] args = { "-cql-style", "" };
        for (ConnectionStyle style : ConnectionStyle.values())
        {
            args[1] = String.format("%s", style.name());
            CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
            SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
            assertNull(style.name(), underTest.password);
            assertNull(style.name(), underTest.username);
            assertNull(style.name(), underTest.authProvider);
            assertEquals(style.name(), ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
            assertEquals(style.name(), Integer.valueOf(8), underTest.connectionsPerHost);
            assertEquals(style.name(), Integer.valueOf(128), underTest.maxPendingPerConnection);
            assertEquals(style.name(), ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
            assertEquals(style.name(), style, underTest.style);
            assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

            TestingResultLogger logger = new TestingResultLogger();
            underTest.printSettings(logger);
            logger.assertEndsWith("Username: *not set*", ()->style.name());
            logger.assertEndsWith("Password: *not set*", ()->style.name());
            logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE, ()->style.name());
            logger.assertEndsWith("Connection Style: " + style, ()->style.name());
            logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED, ()->style.name());
            logger.assertEndsWith("Auth Provider Class: *not set*", ()->style.name());
            logger.assertEndsWith("Max Pending Per Connection: 128", ()->style.name());
            logger.assertEndsWith("Connections Per Host: 8", ()->style.name());
            logger.assertEndsWith("Compression: NONE", ()->style.name());
        }
    }

    @Test
    public void useCompressionTest() throws ParseException
    {
        String[] args = { "-use-compression", "" };
        for (ProtocolOptions.Compression compression : ProtocolOptions.Compression.values())
        {
            args[1] = String.format("%s", compression.name());
            CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
            SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
            assertNull(compression.name(), underTest.password);
            assertNull(compression.name(), underTest.username);
            assertNull(compression.name(), underTest.authProvider);
            assertEquals(compression.name(), ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
            assertEquals(compression.name(), Integer.valueOf(8), underTest.connectionsPerHost);
            assertEquals(compression.name(), Integer.valueOf(128), underTest.maxPendingPerConnection);
            assertEquals(compression.name(), ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
            assertEquals(compression.name(), ConnectionStyle.CQL_PREPARED, underTest.style);
            assertEquals(compression.name(), compression, underTest.compression);

            TestingResultLogger logger = new TestingResultLogger();
            underTest.printSettings(logger);
            logger.assertEndsWith("Username: *not set*", ()->compression.name());
            logger.assertEndsWith("Password: *not set*", ()->compression.name());
            logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE, ()->compression.name());
            logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED, ()->compression.name());
            logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED, ()->compression.name());
            logger.assertEndsWith("Auth Provider Class: *not set*", ()->compression.name());
            logger.assertEndsWith("Max Pending Per Connection: 128", ()->compression.name());
            logger.assertEndsWith("Connections Per Host: 8", ()->compression.name());
            logger.assertEndsWith("Compression: " + (compression == ProtocolOptions.Compression.NONE ? "NONE" : compression), ()->compression.name());
        }
    }

    @Test
    public void authProviderTest() throws ParseException
    {
        String[] args = { "-auth-provider", TestingAuthProvider.class.getName() };
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertNull(underTest.password);
        assertNull(underTest.username);
        assertEquals(TestingAuthProvider.class, underTest.authProvider.getClass());
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *not set*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: " + TestingAuthProvider.class.getName());
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");

        try {
            args = new String[] { "-auth-provider", String.class.getName() };
            commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
            new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // do nothing.
        }

    }

    @Test
    public void connectionsPerHostTest() throws ParseException
    {
        String[] args = {"-connections-per-host", "3"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertNull(underTest.password);
        assertNull(underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(3), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(128), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *not set*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 128");
        logger.assertEndsWith("Connections Per Host: 3");
        logger.assertEndsWith("Compression: NONE");

        try {
            args = new String[] {"-connections-per-host", "-1"};
            commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
            new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
            fail("Should throw ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
            // do nothing.
        }
    }
    @Test
    public void maxPendingConnectionsTest() throws ParseException
    {
        String[] args = {"-max-pending-connections", "3"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertNull(underTest.password);
        assertNull(underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.JAVA_DRIVER_NATIVE, underTest.api);
        assertEquals(Integer.valueOf(8), underTest.connectionsPerHost);
        assertEquals(Integer.valueOf(3), underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *not set*");
        logger.assertEndsWith("API: " + ConnectionAPI.JAVA_DRIVER_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: 3");
        logger.assertEndsWith("Connections Per Host: 8");
        logger.assertEndsWith("Compression: NONE");

        try {
            args = new String[] {"-max-pending-connections", "-1"};
            commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
            new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
            fail("Should throw ParseException");
        } catch (RuntimeException expected) {
            assertEquals(ParseException.class, expected.getCause().getClass());
            // do nothing.
        }
    }

    @Test
    public void simpleNativeTest() throws ParseException
    {
        String[] args = {"-simple-native"};
        CommandLine commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        SettingsMode underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertNull(underTest.password);
        assertNull(underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.SIMPLE_NATIVE, underTest.api);
        assertNull(underTest.connectionsPerHost);
        assertNull(underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        TestingResultLogger logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *not set*");
        logger.assertEndsWith("API: " + ConnectionAPI.SIMPLE_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: *not set*");
        logger.assertEndsWith("Connections Per Host: *not set*");
        logger.assertEndsWith("Compression: NONE");

        // test will all args
        args = new String[] {"-simple-native", "-protocol-version", "4", "-cql-style", "CQL_PREPARED", "-use-compression", "lz4", "-user", "commandLineUser", "-password", "commandLinePwd", "-auth-provider",  TestingAuthProvider.class.getName(),
                         "-max-pending-connections", "500", "-connections-per-host", "10"};
        commandLine = DefaultParser.builder().build().parse(SettingsMode.getOptions(), args);
        underTest = new SettingsMode(commandLine, SettingsCredentialsTest.getSettingsCredentials());
        assertNull(underTest.password);
        assertNull(underTest.username);
        assertNull(underTest.authProvider);
        assertEquals(ConnectionAPI.SIMPLE_NATIVE, underTest.api);
        assertNull(underTest.connectionsPerHost);
        assertNull(underTest.maxPendingPerConnection);
        assertEquals(ProtocolVersion.NEWEST_SUPPORTED, underTest.protocolVersion);
        assertEquals(ConnectionStyle.CQL_PREPARED, underTest.style);
        assertEquals(ProtocolOptions.Compression.NONE, underTest.compression);

        logger = new TestingResultLogger();
        underTest.printSettings(logger);
        logger.assertEndsWith("Username: *not set*");
        logger.assertEndsWith("Password: *not set*");
        logger.assertEndsWith("API: " + ConnectionAPI.SIMPLE_NATIVE);
        logger.assertEndsWith("Connection Style: " + ConnectionStyle.CQL_PREPARED);
        logger.assertEndsWith("Protocol Version: " + ProtocolVersion.NEWEST_SUPPORTED);
        logger.assertEndsWith("Auth Provider Class: *not set*");
        logger.assertEndsWith("Max Pending Per Connection: *not set*");
        logger.assertEndsWith("Connections Per Host: *not set*");
        logger.assertEndsWith("Compression: NONE");
    }

    public static class TestingAuthProvider implements AuthProvider
    {
        @Override
        public Authenticator newAuthenticator(InetSocketAddress inetSocketAddress, String s) throws AuthenticationException
        {
            return null;
        }
    }
}
