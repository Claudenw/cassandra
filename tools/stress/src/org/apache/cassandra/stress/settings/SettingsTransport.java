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
    public static final StressOption<String>  TRANSPORT_PROTOCOL = new StressOption<>(()->"TLS", new Option("ssl-protocol", true, "Connection protocol to use.  Defaults to TLS."));
    public static final StressOption<String>  TRANSPORT_ALGORITHM = new StressOption<>(new Option("ssl-algorithm", true, "Algorithm"));
    static final String[] TRANSPORT_CIPHERS_DEFAULT = { "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"};
    public static final StressOption<String[]>  TRANSPORT_CIPHERS = new StressOption<>(()->TRANSPORT_CIPHERS_DEFAULT,
                                                                                       Option.builder("ssl-ciphers").hasArgs()
                                                                                             .desc(format("A list of encryption suites to use. (Default = %s)",String.join(" ", TRANSPORT_CIPHERS_DEFAULT))).build());
    public final EncryptionOptions encryptionOptions;
    final String truststore;
    final String truststorePassword;
    final String algorithm;
    final String protocol;
    final String[] ciphers;
    final String keystore;
    final String keystorePassword;

    public SettingsTransport(CommandLine commandLine, SettingsCredentials credentials)
    {
        truststore =  TRANSPORT_TRUSTSTORE.extract(commandLine);
        truststorePassword = commandLine.getOptionValue(TRANSPORT_TRUSTSTORE_PASSWORD.option(),credentials.transportTruststorePassword);
        algorithm = TRANSPORT_ALGORITHM.extract(commandLine);
        protocol = TRANSPORT_PROTOCOL.extract(commandLine);
        ciphers = TRANSPORT_CIPHERS.extractArray(commandLine);
        keystore = TRANSPORT_KEYSTORE.extract(commandLine);
        keystorePassword = commandLine.getOptionValue(TRANSPORT_KEYSTORE_PASSWORD.option(),credentials.transportKeystorePassword);

        EncryptionOptions encOptions = new EncryptionOptions().applyConfig();
        if (truststore != null)
        {
            encOptions = encOptions
                         .withEnabled(true)
                         .withTrustStore(truststore)
                         .withTrustStorePassword(truststorePassword)
                         .withAlgorithm(algorithm)
                         .withProtocol(protocol)
                         .withCipherSuites(ciphers);
            if (keystore != null)
            {
                encOptions = encOptions
                             .withKeyStore(keystore)
                             .withKeyStorePassword(keystorePassword);
            }
            else
            {
                // mandatory for SSLFactory.createSSLContext(), see CASSANDRA-9325
                encOptions = encOptions
                             .withKeyStore(truststore)
                             .withKeyStorePassword(encOptions.truststore_password != null ? encOptions.truststore_password : credentials.transportTruststorePassword);
            }
        }
        encryptionOptions = encOptions;
    }

    public EncryptionOptions getEncryptionOptions()
    {
        return encryptionOptions;
    }

    // Option Declarations

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


    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Truststore: %s%n", PrintUtils.printNull(truststore));
        out.printf("  Truststore Password: %s%n", PrintUtils.printSensitive(truststorePassword));
        out.printf("  Keystore: %s%n",  PrintUtils.printNull(keystore));
        out.printf("  Keystore Password: %s%n", PrintUtils.printSensitive(keystorePassword));
        out.printf("  SSL Protocol: %s%n", protocol);
        out.printf("  SSL Algorithm: %s%n", algorithm);
        out.printf("  SSL Ciphers: %s%n", String.join(",",ciphers));
    }
}
