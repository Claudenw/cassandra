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


import java.io.Serializable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY;


public class SettingsTransport extends AbstractSettings implements Serializable
{

    public static final StressOption<String>  TRANSPORT_TRUSTSTORE = new StressOption<>(new Option("ssl-truststore", true, "Full path to truststore."));
    public static final StressOption<String>  TRANSPORT_TRUSTSTORE_PASSWORD = new StressOption<>(new Option("ssl-truststore-password", true, format("Truststore password, when specified, it will override the value in credentials file of key '%s'",
                                                                                                                                                    TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY)));
    public static final StressOption<String>  TRANSPORT_KEYSTORE = new StressOption<>(new Option("ssl-keystore", true, "Full path to keystore."));
    public static final StressOption<String>  TRANSPORT_KEYSTORE_PASSWORD = new StressOption<>(new Option("ssl-keystore-password", true, format("Keystore password, when specified, it will override the value in credentials file for key '%s'",
                                                                                                                                                TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY)));
    public static final StressOption<String>  TRANSPORT_PROTOCOL = new StressOption<>(()->"TLS", new Option("ssl-password", true, "Connection protocol to use.  Defaults to TLS."));
    public static final StressOption<String>  TRANSPORT_ALGORITHM = new StressOption<>(new Option("ssl-algorithm", true, "Algorithm"));
    private static final String[] TRANSPORT_CIPHERS_DEFAULT = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"};
    public static final StressOption<String[]>  TRANSPORT_CIPHERS = new StressOption<>(()->TRANSPORT_CIPHERS_DEFAULT,
                                                                                       new Option("ssl-ciphers", true,
                                                                                                format("Comma delimited list of encryption suites to use. Defualt = %s",
                                                                                                       String.join(",", TRANSPORT_CIPHERS_DEFAULT))));
    private final EncryptionOptions encryptionOptions;
    private final CommandLine commandLine;

    public SettingsTransport(CommandLine commandLine, SettingsCredentials credentials)
    {
        this.commandLine = commandLine;
        EncryptionOptions encOptions = new EncryptionOptions().applyConfig();
        if (commandLine.hasOption(TRANSPORT_TRUSTSTORE.option()))
        {
            encOptions = encOptions
                         .withEnabled(true)
                         .withTrustStore(TRANSPORT_TRUSTSTORE.extract(commandLine))
                         .withTrustStorePassword(commandLine.getOptionValue(TRANSPORT_TRUSTSTORE_PASSWORD.option(),credentials.transportTruststorePassword))
                         .withAlgorithm(TRANSPORT_ALGORITHM.extract(commandLine))
                         .withProtocol(TRANSPORT_PROTOCOL.extract(commandLine))
                         .withCipherSuites(TRANSPORT_CIPHERS.extract(commandLine));
            if (commandLine.hasOption(TRANSPORT_KEYSTORE.option()))
            {
                encOptions = encOptions
                             .withKeyStore(TRANSPORT_KEYSTORE.extract(commandLine))
                             .withKeyStorePassword(commandLine.getOptionValue(TRANSPORT_KEYSTORE_PASSWORD.option(),credentials.transportKeystorePassword));
            }
            else
            {
                // mandatory for SSLFactory.createSSLContext(), see CASSANDRA-9325
                encOptions = encOptions
                             .withKeyStore(encOptions.truststore)
                             .withKeyStorePassword(encOptions.truststore_password != null ? encOptions.truststore_password : credentials.transportTruststorePassword);
            }
        }
        encryptionOptions = encOptions;
    }

    public EncryptionOptions getEncryptionOptions()
    {
        return encryptionOptions;
    }

    public static Options getOptions()
    {
        return new Options()
               .addOption(TRANSPORT_TRUSTSTORE.option())
               .addOption(TRANSPORT_TRUSTSTORE_PASSWORD.option())
               .addOption(TRANSPORT_KEYSTORE.option())
               .addOption(TRANSPORT_KEYSTORE_PASSWORD .option())
               .addOption(TRANSPORT_PROTOCOL.option())
               .addOption(TRANSPORT_ALGORITHM.option())
               .addOption(TRANSPORT_CIPHERS.option());
    }
    // Option Declarations
//
//    static class TOptions extends GroupedOptions implements Serializable
//    {
//        final OptionSimple trustStore = new OptionSimple("truststore=", ".*", null, "SSL: full path to truststore", false);
//        final OptionSimple trustStorePw = new OptionSimple("truststore-password=", ".*", null,
//                                                           format("SSL: truststore password, when specified, it will override the value in credentials file of key '%s'",
//                                                                  TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY), false);
//        final OptionSimple keyStore = new OptionSimple("keystore=", ".*", null, "SSL: full path to keystore", false);
//        final OptionSimple keyStorePw = new OptionSimple("keystore-password=", ".*", null,
//                                                         format("SSL: keystore password, when specified, it will override the value in credentials file for key '%s'",
//                                                                TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY), false);
//        final OptionSimple protocol = new OptionSimple("ssl-protocol=", ".*", "TLS", "SSL: connection protocol to use", false);
//        final OptionSimple alg = new OptionSimple("ssl-alg=", ".*", null, "SSL: algorithm", false);
//        final OptionSimple ciphers = new OptionSimple("ssl-ciphers=", ".*",
//                                                      "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA",
//                                                      "SSL: comma delimited list of encryption suites to use", false);
//
//        @Override
//        public List<? extends Option> options()
//        {
//            return Arrays.asList(trustStore, trustStorePw, keyStore, keyStorePw, protocol, alg, ciphers);
//        }
//    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Truststore: %s%n", TRANSPORT_TRUSTSTORE.extract(commandLine));
        out.printf("  Truststore Password: %s%n", commandLine.hasOption(TRANSPORT_TRUSTSTORE_PASSWORD.option()) ? "*supressed*" : null);
        out.printf("  Keystore: %s%n",  TRANSPORT_KEYSTORE.extract(commandLine));
        out.printf("  Keystore Password: %s%n", commandLine.hasOption(TRANSPORT_KEYSTORE_PASSWORD.option()) ? "*supressed*" : null);
        out.printf("  SSL Protocol: %s%n", TRANSPORT_PROTOCOL.extract(commandLine));
        out.printf("  SSL Algorithm: %s%n", TRANSPORT_ALGORITHM.extract(commandLine));
        out.printf("  SSL Ciphers: %s%n", String.join(",",TRANSPORT_CIPHERS.extract(commandLine)));
    }
//
//    public static SettingsTransport get(Map<String, String[]> clArgs, SettingsCredentials credentials)
//    {
//        String[] params = clArgs.remove("-transport");
//        if (params == null)
//            return new SettingsTransport(new TOptions(), credentials);
//
//        GroupedOptions options = GroupedOptions.select(params, new TOptions());
//        if (options == null)
//        {
//            printHelp();
//            System.out.println("Invalid -transport options provided, see output for valid options");
//            System.exit(1);
//        }
//        return new SettingsTransport((TOptions) options, credentials);
//    }
//
//    public static void printHelp()
//    {
//        GroupedOptions.printOptions(System.out, "-transport", new TOptions());
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
