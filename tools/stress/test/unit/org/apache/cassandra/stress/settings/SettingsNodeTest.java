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
import org.apache.commons.cli.DefaultParser;
import org.junit.Test;

import static org.junit.Assert.*;

public class SettingsNodeTest
{
    @Test
    public void testDefaults() throws Exception
    {
        CommandLine commandLine = DefaultParser.builder().build().parse(StressSettings.getOptions(), new String[0]);
        SettingsNode settingsNode = new SettingsNode(commandLine);
        assertEquals(null, settingsNode.datacenter);
    }

    @Test
    public void testOveridingDataCenter() throws Exception
    {
        CommandLine commandLine = DefaultParser.builder().build().parse(StressSettings.getOptions(), new String[]{
        "-" + SettingsNode.NODE_DATACENTER.key(), "dc1"
        });
        SettingsNode settingsNode = new SettingsNode(commandLine);
        assertEquals("dc1", settingsNode.datacenter);
    }
}
