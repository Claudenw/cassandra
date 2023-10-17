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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Directories;

public class RelocatingFileSystemMapper implements FileSystemMapperHandler
{

    private static final Logger logger = LoggerFactory.getLogger(RelocatingFileSystemMapper.class);

    private final Path basePath;
    private final String keyspace;
    private final String tableName;

    public RelocatingFileSystemMapper(String basePath, String keySpace, String tableName) {
        Objects.requireNonNull("basePath", "basePath may not be null");
        Objects.requireNonNull("keyspace", "Keyspace may not be null");
        this.basePath = FileSystems.getDefault().getPath(basePath);
        this.keyspace = keySpace;
        this.tableName = tableName;
    }

    // for ParameterizedClass use
    public RelocatingFileSystemMapper(Map<String,String> args) throws IOException
    {
        this(args.get("basePath"), args.get("keyspace"), args.get("tableName"));
    }

    @Override
    public void extractDirectories(Collection<Directories.DataDirectory> collection)
    {
        collection.add(new Directories.DataDirectory(basePath));
    }

    @Override
    public Path getPath(String keyspace, String tableName)
    {
        if (this.keyspace.equals(keyspace) && (StringUtils.isBlank(this.tableName) || this.tableName.equals(tableName)))
        {
            return basePath;
        }
        return null;
    }
}
