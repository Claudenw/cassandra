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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelocatingFileSystemMapper implements FileSystemMapperHandler
{

    private static final Logger logger = LoggerFactory.getLogger(RelocatingFileSystemMapper.class);

    private final String source;
    private final String dest;
    /**
     * Creates the ChannelProxyFactory.
     *
     * @param source The source directory
     * @param dest The destination directory
     */
    protected RelocatingFileSystemMapper(String source, String dest) {
        this.source = source;
        this.dest = dest;
    }

    // for ParameterizedClass use
    public RelocatingFileSystemMapper(Map<String,String> args) throws IOException
    {
        this(args.get("source"), args.get("dest"));
    }


    @Override
    public Path getPath(String first, String ... more)
    {
        String pathStr = FileSystems.getDefault().getPath(first, more).toString();
        return pathStr.startsWith(source) ?
            FileSystems.getDefault().getPath(dest, pathStr.substring(source.length())) :
               null;
    }

}
