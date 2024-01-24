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

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

public class OptionsTests {


    public static void main(String[] args) {
        Options options = AbstractSettings.allOptions();
/*
        SettingsColumn.getOptions().getOptions().forEach(options::addOption);
        SettingsCommand.getOptions().getOptions().forEach(options::addOption);
        SettingsPopulation.getOptions().getOptions().forEach(options::addOption);
        SettingsPort.getOptions().getOptions().forEach(options::addOption);
        SettingsRate.getOptions().getOptions().forEach(options::addOption);
*/
        HelpFormatter formatter = new HelpFormatter();

        formatter.printHelp("myapp", "HEADER", options, "FOOTER", true);
    }
}
