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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import org.apache.cassandra.stress.util.ResultLogger;

import static java.lang.String.format;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_PASSWORD_PROPERTY_KEY;
import static org.apache.cassandra.stress.settings.SettingsCredentials.JMX_USERNAME_PROPERTY_KEY;


public class SettingsJMX extends AbstractSettings
{
    public static final StressOption<String> JMX_USER = new StressOption<>(new Option("jmx-user", true, format("Username for JMX connection, when specified, it will override the value in credentials file for key '%s'.", JMX_USERNAME_PROPERTY_KEY)));
    public static final StressOption<String> JMX_PASSWORD = new StressOption<>(new Option("jmx-password", true, format("Password for JMX connection, when specified, it will override the value in credentials file for key '%s'.", JMX_PASSWORD_PROPERTY_KEY)));
    public final String user;
    public final String password;

    public SettingsJMX(CommandLine commandLine, SettingsCredentials credentials)
    {
        this.user = commandLine.getOptionValue(JMX_USER.option(), credentials.jmxUsername);
        this.password = commandLine.getOptionValue(JMX_PASSWORD.option(), credentials.jmxPassword);
    }

    // Option Declarations

    public static Options getOptions() {
        return new Options()
               .addOption(JMX_USER.option())
               .addOption(JMX_PASSWORD.option())
        ;
    }

//
    // CLI Utility Methods
    public void printSettings(ResultLogger out)
    {
        out.printf("  Username: %s%n", user);
        out.printf("  Password: %s%n", (password == null ? "*not set*" : "*suppressed*"));
    }
}
