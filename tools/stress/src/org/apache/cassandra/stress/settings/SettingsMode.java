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

    public static final StressOption<ProtocolVersion> MODE_PROTOCOL_VERSION = new StressOption<>(()->ProtocolVersion.NEWEST_SUPPORTED, Option.builder("protocol-version").desc(format("CQL Protocol Version. (Defaults to %s)", ProtocolVersion.NEWEST_SUPPORTED.toInt()))
                                                                                                                                             .type(ProtocolVersion.class).converter(s-> ProtocolVersion.fromInt(Integer.parseInt(s))).build());
    public static final StressOption<String> MODE_USE_PREPARED = new StressOption<>(new Option("use-prepared", "Use prepared statements."));
    public static final StressOption<String> MODE_USE_UNPREPARED = new StressOption<>(new Option("use-unprepared", "Use unprepared statements."));
    public static final StressOption<ProtocolOptions.Compression> MODE_COMPRESSION = new StressOption<>(Option.builder("use-compression").desc(format("Use compression. Valid options are: %s", enumOptionString(ProtocolOptions.Compression.LZ4)))
                                                                                                        .hasArg().converter(s -> ProtocolOptions.Compression.valueOf(s.toUpperCase())).build());
    public static final StressOption<String> MODE_USER = new StressOption<>(new Option("user", true, format("CQL user, when specified, it will override the value in credentials file for key '%s'.", CQL_USERNAME_PROPERTY_KEY)));
    public static final StressOption<String> MODE_PASSWORD = new StressOption<>(new Option("password", true, format("CQL password, when specified, it will override the value in credentials file for key '%s'.", CQL_PASSWORD_PROPERTY_KEY)));
    public static final StressOption<Class<? extends AuthProvider>> MODE_AUTHPROVIDER = new StressOption<>(Option.builder("auth-provider").hasArg().argName("class").desc("Fully qualified name of an implementation of com.datastax.driver.core.AuthProvider")
                                                                                                                 .type(Class.class).build());
    public static final StressOption<String> MODE_SIMPLE_NATIVE= new StressOption<>(new Option("simple-native", "Simple native opitons"));
    private static final int MODE_MAX_PENDING_DEFAULT = 128;
    public static final StressOption<Integer> MODE_MAX_PENDING_CONNECTIONS= new StressOption<>(()->MODE_MAX_PENDING_DEFAULT, POSITIVE_VERIFIER, Option.builder("max-pending-connections").hasArg().desc(format("Maximum pending requests per connection. (Default value=%s)", MODE_MAX_PENDING_DEFAULT)).type(Integer.class).build());
    private static final int MODE_CONNECTIONS_PER_HOST_DEFAULT = 8;
    public static final StressOption<Integer> MODE_CONNECTIONS_PER_HOST= new StressOption<>(()->MODE_CONNECTIONS_PER_HOST_DEFAULT, POSITIVE_VERIFIER, Option.builder("connections-per-host").hasArg().desc(format("Number of connections per host. (Default value=%s)", MODE_CONNECTIONS_PER_HOST_DEFAULT)).type(Integer.class).build());
    public final ConnectionAPI api;
    public final ConnectionStyle style;
    public final ProtocolVersion protocolVersion;

    public final String username;
    public final String password;

    public final AuthProvider authProvider;

    public final Integer maxPendingPerConnection;
    public final Integer connectionsPerHost;

    private final ProtocolOptions.Compression compression;


    public SettingsMode(CommandLine commandLine, SettingsCredentials credentials)
    {
        final Class<? extends AuthProvider> authProviderClass;

        if (commandLine.hasOption(MODE_SIMPLE_NATIVE.option()))
        {
            protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
            api = ConnectionAPI.SIMPLE_NATIVE;
            style = commandLine.hasOption(MODE_USE_PREPARED.option()) ? ConnectionStyle.CQL_PREPARED : ConnectionStyle.CQL;
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
            style = commandLine.hasOption(MODE_USE_UNPREPARED.option()) ? ConnectionStyle.CQL : ConnectionStyle.CQL_PREPARED;
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

    public ProtocolOptions.Compression compression()
    {
        return compression;
    }

    public static Options getOptions() {
        return new Options()
               .addOption(MODE_PROTOCOL_VERSION.option())
        .addOption(MODE_USE_PREPARED.option())
        .addOption(MODE_USE_UNPREPARED.option())
        .addOption(MODE_COMPRESSION.option())
        .addOption(MODE_USER.option())
        .addOption(MODE_PASSWORD.option())
        .addOption(MODE_AUTHPROVIDER.option())
        .addOption(MODE_MAX_PENDING_CONNECTIONS.option())
        .addOption(MODE_CONNECTIONS_PER_HOST.option())
        .addOption(MODE_SIMPLE_NATIVE.option())
        ;
    }

//    private static class Cql3Options extends GroupedOptions
//    {
//        final OptionSimple protocolVersion = new OptionSimple("protocolVersion=", "[2-5]+", "NEWEST_SUPPORTED", "CQL Protocol Version", false);
//        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "Use prepared statements", false);
//        final OptionSimple useUnPrepared = new OptionSimple("unprepared", "", null, "Use unprepared statements", false);
//        final OptionSimple useCompression = new OptionSimple("compression=", "none|lz4|snappy", "none", "", false);
//        final OptionSimple port = new OptionSimple("port=", "[0-9]+", "9046", "", false);
//        final OptionSimple user = new OptionSimple("user=", ".+", null,
//                                                   format("CQL user, when specified, it will override the value in credentials file for key '%s'", CQL_USERNAME_PROPERTY_KEY),
//                                                   false);
//        final OptionSimple password = new OptionSimple("password=", ".+", null,
//                                                       format("CQL password, when specified, it will override the value in credentials file for key '%s'", CQL_PASSWORD_PROPERTY_KEY),
//                                                       false);
//        final OptionSimple authProvider = new OptionSimple("auth-provider=", ".*", null, "Fully qualified implementation of com.datastax.driver.core.AuthProvider", false);
//        final OptionSimple maxPendingPerConnection = new OptionSimple("maxPending=", "[0-9]+", "128", "Maximum pending requests per connection", false);
//        final OptionSimple connectionsPerHost = new OptionSimple("connectionsPerHost=", "[0-9]+", "8", "Number of connections per host", false);
//        final OptionSimple simplenative = new OptionSimple("simplenative", "", null, "", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(user, password, port, authProvider,maxPendingPerConnection,
//                                 useCompression, connectionsPerHost, usePrepared, useUnPrepared,
//                                 protocolVersion, simplenative);
//        }
//    }
    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  API: %s%n", api);
        out.printf("  Connection Style: %s%n", style);
        out.printf("  Protocol Version: %s%n", protocolVersion);
        out.printf("  Username: %s%n", username);
        out.printf("  Password: %s%n", (password == null ? password : "*suppressed*"));
        out.printf("  Auth Provide Class: %s%n", authProvider);
        out.printf("  Max Pending Per Connection: %d%n", maxPendingPerConnection);
        out.printf("  Connections Per Host: %d%n", connectionsPerHost);
        out.printf("  Compression: %s%n", compression);
    }

//    public static SettingsMode get(Map<String, String[]> clArgs, SettingsCredentials credentials)
//    {
//        String[] params = clArgs.remove("-mode");
//        List<String> paramList = new ArrayList<>();
//        if (params == null)
//        {
//            Cql3Options opts = new Cql3Options();
//            opts.accept("prepared");
//            return new SettingsMode(opts, credentials);
//        }
//        for (String item : params)
//        {
//            // Warn on obsolete arguments, to be removed in future release
//            if (item.equals("cql3") || item.equals("native"))
//            {
//                System.err.println("Warning: ignoring deprecated parameter: " + item);
//            }
//            else
//            {
//                paramList.add(item);
//            }
//        }
//        if (paramList.contains("prepared") && paramList.contains("unprepared"))
//        {
//            System.err.println("Warning: can't specify both prepared and unprepared, using prepared");
//            paramList.remove("unprepared");
//        }
//        String[] updated = paramList.toArray(new String[paramList.size()]);
//        GroupedOptions options = new Cql3Options();
//        GroupedOptions.select(updated, options);
//        return new SettingsMode(options, credentials);
//    }
//
//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-mode", new Cql3Options());
//    }
//
//    public static Runnable helpPrinter()
//    {
//        return new Runnable()
//        {
//            @Override
//            public void run()
//            {
//                printHelp();
//            }
//        };
//    }
}
