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

package org.apache.cassandra.io.util;

import java.util.Collections;
import java.util.function.Function;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;

public interface ChannelProxyFactory extends Function<File, ChannelProxy>
{
    static ChannelProxyFactory instance() {
        Config conf = DatabaseDescriptor.getRawConfig();
        return conf.channel_proxy_factory == null ? null : ParameterizedClass.newInstance(conf.channel_proxy_factory, Collections.emptyList());
    }
    /**
     * Creates a ChannelProxy for the specified table name in the specifies keyspace.
     * @param ksname the keyspace name.
     * @param cfname the table name.
     * @param fileName the name of the sstable file.
     * @return the specified channel proxy or null if it can not be created.
     */
    ChannelProxy create(String ksname, String cfname, String fileName);

    default ChannelProxy apply(File file) {
        Pair<Descriptor,?> pair = Descriptor.fromFileWithComponent(file, true);
        return create(pair.left.ksname, pair.left.cfname, file.name());
    }
}
