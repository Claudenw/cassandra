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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.AbstractSequentialList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;

public final class FileSystemMapper
{

    private static final Logger logger = LoggerFactory.getLogger(FileSystemMapper.class);

    private static FileSystemMapper INSTANCE;

    private final AbstractSequentialList<FileSystemMapperHandler> factories;


    /**
     * Creates a ProxyFactory that matches the defaults for read and write for version 4.x
     * @return The default ChannelProxyFactory.
     */
    private static FileSystemMapper createDefault() {
        Config conf = DatabaseDescriptor.getRawConfig();
        FileSystemMapper factory = new FileSystemMapper();
        if (conf.file_system_mapper_handler != null) {
            factory.factories.add(ParameterizedClass.newInstance(conf.file_system_mapper_handler, Collections.emptyList()));
        }
        return factory;
    }

    /**
     * Returns the registered ChannelProxyFactory.  Will create the default if no instance has been set.
     * @return The registered ChannelProxyFactory.
     */
    public static FileSystemMapper instance() {
        FileSystemMapper result = INSTANCE;
        if (result == null) {
            INSTANCE = result = createDefault();
            if (logger.isDebugEnabled())
            {
                logger.debug("Created ChannelProxyFactory of class: {}", result.getClass().getName());
            }
        }
        return result;
    }

    /**
     * Sets the instance of the ChannelProxyFactory.
     * @param instance the instance to use.
     */
    public static void setInstance(FileSystemMapper instance) {
        INSTANCE = instance;
    }

    public static void addHandler(FileSystemMapperHandler handler) {
        AbstractSequentialList fList = instance().factories;
        if (fList.size() == 0)
        {
            fList.add(handler);
        } else
        {
            fList.add(fList.size() - 1, handler);
        }
    }


    public FileSystemMapper(FileSystemMapperHandler... factories) {
        this.factories = new LinkedList<>(Arrays.asList(factories));
    }

    private Path findPath(String first, String ... more) {
        Optional<Path> path = factories.stream().map(h ->h.getPath(first,more)).filter(Objects::nonNull).findFirst();
        if (logger.isDebugEnabled() && path.isPresent()) {
            StringBuilder sb = new StringBuilder();
            sb.append(first).append(" ");
            if (more != null)
            {
                Arrays.stream(more).forEach(s -> sb.append(s).append(" "));
            }
            logger.debug("Selected %s selected file system %s", sb, path.get());
        }
        return path.isPresent() ? path.get() : FileSystems.getDefault().getPath(first, more);
    }

    public static Path getPath(String first, String... more)
    {
        return instance().findPath(first, more);
    }
}
