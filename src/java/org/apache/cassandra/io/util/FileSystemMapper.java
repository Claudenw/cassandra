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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Directories;

public final class FileSystemMapper
{

    private static final Logger logger = LoggerFactory.getLogger(FileSystemMapper.class);

    private static FileSystemMapper INSTANCE;

    private final AbstractSequentialList<FileSystemMapperHandler> factories;

    /**
     * Returns the registered ChannelProxyFactory.  Will create the default if no instance has been set.
     * @return The registered ChannelProxyFactory.
     */
    public static FileSystemMapper instance() {
        FileSystemMapper result = INSTANCE;
        if (result == null) {
            result = new FileSystemMapper();
            Config conf = DatabaseDescriptor.getRawConfig();
            if (conf != null && conf.file_system_mapper_handler != null) {
                result.factories.add(ParameterizedClass.newInstance(conf.file_system_mapper_handler, Collections.emptyList()));

                if (logger.isDebugEnabled())
                {
                    StringBuilder sb = new StringBuilder("Created FileSystemMapper with mapper handlers of:\n");
                    result.factories.stream().forEach( x -> sb.append( x.getClass().getName()).append("\n"));
                    sb.append( "--- END OF LIST ---");
                    logger.debug(sb.toString());
                }
            }
            if (conf == null && logger.isDebugEnabled())
            {
                logger.debug("Created Temporary FileSystemMapper");
            }  else
            {
                INSTANCE = result;
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

    public static void addHandler(FileSystemMapperHandler handler)
    {
        instance().factories.add(handler);
    }


    public FileSystemMapper(FileSystemMapperHandler... factories)
    {
        this.factories = new LinkedList<>(Arrays.asList(factories));
    }

    private void extract(Collection<Directories.DataDirectory> collection)
    {
        factories.stream().forEach( x -> x.extractDirectories(collection));
    }

    private Path findPath(String keySpace, String tableName)
    {
        Optional<Path> result = factories.stream().map( x -> x.getPath(keySpace, tableName)).filter(Objects::nonNull).findFirst();
        return result.isPresent() ? result.get() : null;
    }

    public static void extractDirectories(Collection<Directories.DataDirectory> collection)
    {
        instance().extract(collection);
    }

    public static Path getPath(String keyspace, String tableName) {
        return instance().findPath(keyspace, tableName);
    }

    /**
     * Parses Path and String  combinations into a collections of strings and then uses thoem to create
     * paths within filesystems.
     */
    public static class PathParser
    {
        private final static String separator = FileSystems.getDefault().getSeparator();
        private final String first;
        @Nullable
        private final String[]  rest;

        private static String[] makeRelative(String path)
        {
            if (path == null) {
                return null;
            }
            String relative = path.startsWith(separator) ? path.substring(separator.length()) : path;
            return path.split(Pattern.quote(separator));
        }

        private PathParser(boolean startAtRoot, String[] part1, String[] part2) {

            Queue<String> args = new LinkedList<>();
            if (part1 != null) {
                args.addAll(Arrays.asList(part1));
            }
            if (part2 != null) {
                args.addAll(Arrays.asList(part2));
            }
            this.first = (startAtRoot ? FileSystems.getDefault().getSeparator() : "") + args.poll();
            this.rest = args.size()==0 ? null : args.toArray(new String[0]);
        }

        PathParser(Path path, String rest) {
            // use path based separator for the path parsing.
            this(path==null ? false : path.startsWith(path.getFileSystem().getSeparator()), path == null ? null : path.toString().split(Pattern.quote(path.getFileSystem().getSeparator())), makeRelative(rest));
        }

        PathParser(String first, String rest) {
            this(first == null ? false : first.startsWith(separator), first == null ? null : first.split(Pattern.quote(separator)), makeRelative(rest));
        }

        PathParser(String first, String[] rest) {
            this(first == null ? false : first.startsWith(separator), first == null ? null : first.split(Pattern.quote(separator)), rest);
        }

        public String first() {
            return first;
        }

        public String[] rest() {
            return rest;
        }

        @Override
        public String toString() {
            return rest == null ? first : String.format("%s %s", first , StringUtils.join(rest, ' '));
        }
    }
}
