package org.apache.cassandra.stress.settings;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */



import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.CQL_USERNAME_PROPERTY_KEY;


public class SettingsMode extends AbstractSettings
{

    public static final StressOption<ProtocolVersion> MODE_PROTOCOL_VERSION = new StressOption<>(()->ProtocolVersion.NEWEST_SUPPORTED,
                                                                                                 Option.builder("protocol-version").hasArg()
                                                                                                       .desc(format("CQL Protocol Version. (Defaults to %s)", ProtocolVersion.NEWEST_SUPPORTED.toInt()))
                                                                                                       .type(ProtocolVersion.class).converter(s-> ProtocolVersion.fromInt(Integer.parseInt(s))).build());
    public static final StressOption<ConnectionStyle> MODE_CQL_STYLE = new StressOption<>(Option.builder("cql-style").hasArg()
                                                                                                .desc( "CQL connections style.  Valid options are: "+enumOptionString(ConnectionStyle.CQL_PREPARED))
                                                                                                .converter(s->ConnectionStyle.valueOf(s.toUpperCase())).build());
    public static final StressOption<ProtocolOptions.Compression> MODE_COMPRESSION = new StressOption<>(()->ProtocolOptions.Compression.NONE,
                                                                                                        Option.builder("use-compression").desc(format("Use compression. Valid options are: %s", enumOptionString(ProtocolOptions.Compression.LZ4)))
                                                                                                        .hasArg().converter(s -> ProtocolOptions.Compression.valueOf(s.toUpperCase())).build());
    public static final StressOption<String> MODE_USER = new StressOption<>(new Option("user", true, format("CQL user, when specified, it will override the value in credentials file for key '%s'.", CQL_USERNAME_PROPERTY_KEY)));
    public static final StressOption<String> MODE_PASSWORD = new StressOption<>(new Option("password", true, format("CQL password, when specified, it will override the value in credentials file for key '%s'.", CQL_PASSWORD_PROPERTY_KEY)));
    public static final StressOption<Class<? extends AuthProvider>> MODE_AUTHPROVIDER = new StressOption<>(Option.builder("auth-provider").hasArg().argName("class")
                                                                                                                 .desc("Fully qualified name of an implementation of com.datastax.driver.core.AuthProvider")
                                                                                                                 .type(Class.class).build());
    public static final StressOption<String> MODE_SIMPLE_NATIVE= new StressOption<>(new Option("simple-native", "Simple native options"));
    private static final int MODE_MAX_PENDING_DEFAULT = 128;
    public static final StressOption<Integer> MODE_MAX_PENDING_CONNECTIONS= new StressOption<>(()->MODE_MAX_PENDING_DEFAULT,
                                                                                               POSITIVE_VERIFIER,
                                                                                               Option.builder("max-pending-connections").hasArg()
                                                                                                     .desc(format("Maximum pending requests per connection. Only valid with -simple-native. (Default value=%s)", MODE_MAX_PENDING_DEFAULT)).type(Integer.class).build());
    private static final int MODE_CONNECTIONS_PER_HOST_DEFAULT = 8;
    public static final StressOption<Integer> MODE_CONNECTIONS_PER_HOST= new StressOption<>(()->MODE_CONNECTIONS_PER_HOST_DEFAULT,
                                                                                            POSITIVE_VERIFIER,
                                                                                            Option.builder("connections-per-host").hasArg()
                                                                                                  .desc(format("Number of connections per host. Onluy valid with -simple-native. (Default value=%s)", MODE_CONNECTIONS_PER_HOST_DEFAULT))
                                                                                                  .type(Integer.class).build());


    public final ConnectionAPI api;
    public final ConnectionStyle style;
    public final ProtocolVersion protocolVersion;

    public final String username;
    public final String password;

    public final AuthProvider authProvider;

    public final Integer maxPendingPerConnection;
    public final Integer connectionsPerHost;

    public final ProtocolOptions.Compression compression;


    public SettingsMode(CommandLine commandLine, SettingsCredentials credentials)
    {
        final Class<? extends AuthProvider> authProviderClass;

        if (commandLine.hasOption(MODE_SIMPLE_NATIVE.option()))
        {
            protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
            api = ConnectionAPI.SIMPLE_NATIVE;
            style = MODE_CQL_STYLE.extract(commandLine, ConnectionStyle.CQL);
            compression = ProtocolOptions.Compression.NONE;
            username = null;
            password = null;
            authProvider = null;
            maxPendingPerConnection = null;
            connectionsPerHost = null;
        }
        else
        {
            protocolVersion = MODE_PROTOCOL_VERSION.extract(commandLine);
            api = ConnectionAPI.JAVA_DRIVER_NATIVE;
            style = MODE_CQL_STYLE.extract(commandLine, ConnectionStyle.CQL_PREPARED);
            compression = MODE_COMPRESSION.extract(commandLine);
            username = commandLine.getOptionValue(MODE_USER.option(), credentials.cqlUsername);
            password = commandLine.getOptionValue(MODE_PASSWORD.option(), credentials.cqlPassword);
            maxPendingPerConnection = MODE_MAX_PENDING_CONNECTIONS.extract(commandLine);
            connectionsPerHost = MODE_CONNECTIONS_PER_HOST.extract(commandLine);
            authProviderClass = MODE_AUTHPROVIDER.extract(commandLine);
            if (authProviderClass != null)
            {
                try
                {
                    if (!AuthProvider.class.isAssignableFrom(authProviderClass))
                        throw new IllegalArgumentException(authProviderClass.getName() + " is not a valid auth provider");
                    // check we can instantiate it
                    if (PlainTextAuthProvider.class.equals(authProviderClass))
                    {
                        authProvider = (AuthProvider) authProviderClass.getConstructor(String.class, String.class).newInstance(username, password);
                    }
                    else
                    {
                        authProvider = (AuthProvider) authProviderClass.newInstance();
                    }
                }
                catch (Exception e)
                {
                    throw new IllegalArgumentException("Invalid auth provider class: " + authProviderClass.getName(), e);
                }
            }
            else
            {
                authProvider = null;
            }
        }
    }

    public static Options getOptions() {
        return new Options()
               .addOption(MODE_PROTOCOL_VERSION.option())
        .addOption(MODE_CQL_STYLE.option())
        .addOption(MODE_COMPRESSION.option())
        .addOption(MODE_USER.option())
        .addOption(MODE_PASSWORD.option())
        .addOption(MODE_AUTHPROVIDER.option())
        .addOption(MODE_MAX_PENDING_CONNECTIONS.option())
        .addOption(MODE_CONNECTIONS_PER_HOST.option())
        .addOption(MODE_SIMPLE_NATIVE.option())
        ;
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  API: %s%n", api);
        out.printf("  Connection Style: %s%n", style);
        out.printf("  Protocol Version: %s%n", protocolVersion);
        out.printf("  Username: %s%n", PrintUtils.printNull(username));
        out.printf("  Password: %s%n", PrintUtils.printSensitive(password));
        out.printf("  Auth Provider Class: %s%n", PrintUtils.printNull(authProvider));
        out.printf("  Max Pending Per Connection: %s%n", PrintUtils.printNull(maxPendingPerConnection));
        out.printf("  Connections Per Host: %s%n", PrintUtils.printNull(connectionsPerHost));
        out.printf("  Compression: %s%n", compression == ProtocolOptions.Compression.NONE ? "NONE" : compression);
    }
}
