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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.cassandra.stress.util.ResultLogger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import static java.lang.String.format;

public class SettingsCredentials extends AbstractSettings
{
    public static final String CQL_USERNAME_PROPERTY_KEY = "cql.username";
    public static final String CQL_PASSWORD_PROPERTY_KEY = "cql.password";
    public static final String JMX_USERNAME_PROPERTY_KEY = "jmx.username";
    public static final String JMX_PASSWORD_PROPERTY_KEY = "jmx.password";
    public static final String TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY = "transport.truststore.password";
    public static final String TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY = "transport.keystore.password";
    public static final StressOption<String> CREDENTIAL_FILE = new StressOption<>(Option.builder("credential-file").hasArg().argName("file").desc(format("File is supposed to be a standard property file with '%s', '%s', '%s', '%s', '%s', and '%s' as keys. " +
                                                                                                                                                         "The values for these keys will be overriden by their command-line counterparts when specified.%n",
                                                                                                                                                         SettingsCredentials.CQL_USERNAME_PROPERTY_KEY,
                                                                                                                                                         SettingsCredentials.CQL_PASSWORD_PROPERTY_KEY,
                                                                                                                                                         SettingsCredentials.JMX_USERNAME_PROPERTY_KEY,
                                                                                                                                                         SettingsCredentials.JMX_PASSWORD_PROPERTY_KEY,
                                                                                                                                                         SettingsCredentials.TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY,
                                                                                                                                                         SettingsCredentials.TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY)).build());

    private final String file;

    public final String cqlUsername;
    public final String cqlPassword;
    public final String jmxUsername;
    public final String jmxPassword;
    public final String transportTruststorePassword;
    public final String transportKeystorePassword;

    public SettingsCredentials(CommandLine commandLine)
    {
        this.file = commandLine.getOptionValue(CREDENTIAL_FILE.option());
        if (commandLine.hasOption(CREDENTIAL_FILE.option())) {
            Properties properties = new Properties();
            try (InputStream is = new FileInputStream(new org.apache.cassandra.io.util.File(file).toJavaIOFile())) {
                    properties.load(is);
                    cqlUsername = properties.getProperty(CQL_USERNAME_PROPERTY_KEY);
                    cqlPassword = properties.getProperty(CQL_PASSWORD_PROPERTY_KEY);
                    jmxUsername = properties.getProperty(JMX_USERNAME_PROPERTY_KEY);
                    jmxPassword = properties.getProperty(JMX_PASSWORD_PROPERTY_KEY);
                    transportTruststorePassword = properties.getProperty(TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY);
                    transportKeystorePassword = properties.getProperty(TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY);
            } catch (Exception e) {
                throw asRuntimeException(e);
            }
        } else {
                cqlUsername = null;
                cqlPassword = null;
                jmxUsername = null;
                jmxPassword = null;
                transportTruststorePassword = null;
                transportKeystorePassword = null;
        }
    }

    public static Options getOptions()
    {
        return new Options()
               .addOption(CREDENTIAL_FILE.option());
    }

    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  File: %s%n", file == null ? "*not set*" : file);
        out.printf("  CQL username: %s%n", cqlUsername == null ? "*not set*" : cqlUsername);
        out.printf("  CQL password: %s%n", cqlPassword == null ? "*not set*" : "*suppressed*");
        out.printf("  JMX username: %s%n", jmxUsername == null ? "*not set*" : jmxUsername);
        out.printf("  JMX password: %s%n", jmxPassword == null ? "*not set*" : "*suppressed*");
        out.printf("  Transport truststore password: %s%n", transportTruststorePassword == null ? "*not set*" : "*suppressed*");
        out.printf("  Transport keystore password: %s%n", transportKeystorePassword == null ? "*not set*" : "*suppressed*");
    }

//    public static SettingsCredentials get(Map<String, String[]> clArgs)
//    {
//        String[] params = clArgs.remove("-credentials-file");
//        if (params == null)
//            return new SettingsCredentials(null);
//
//        if (params.length != 1)
//        {
//            printHelp();
//            System.out.println("Invalid -credentials-file option provided, see output for valid options");
//            System.exit(1);
//        }
//
//        return new SettingsCredentials(params[0]);
//    }
//
//    public static void printHelp()
//    {
//        System.out.println("Usage: -credentials-file <file> ");
//        System.out.printf("File is supposed to be a standard property file with '%s', '%s', '%s', '%s', '%s', and '%s' as keys. " +
//                          "The values for these keys will be overriden by their command-line counterparts when specified.%n",
//                          CQL_USERNAME_PROPERTY_KEY,
//                          CQL_PASSWORD_PROPERTY_KEY,
//                          JMX_USERNAME_PROPERTY_KEY,
//                          JMX_PASSWORD_PROPERTY_KEY,
//                          TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY,
//                          TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY);
//    }
//
//    public static Runnable helpPrinter()
//    {
//        return SettingsCredentials::printHelp;
//    }
}
