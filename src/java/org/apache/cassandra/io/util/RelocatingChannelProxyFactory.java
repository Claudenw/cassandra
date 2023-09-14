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
import java.util.Map;
import java.util.function.Function;

public class RelocatingChannelProxyFactory  extends ChannelProxyFactory {

    private static class ReaderFactory implements Function<File, ChannelProxy> {
        private final String source;
        private final String dest;
        ReaderFactory(String source, String dest) {
            this.source = source;
            this.dest = dest;
        }

        @Override
        public ChannelProxy apply(File file) {
            File working = file;
            if (file.path().startsWith(source))
            {

                working=new File(dest, file.path().substring(source.length()));
            }
            return new ChannelProxy(working);
        }
    }

    private static class WriterFactory implements Function<File, ChannelProxy> {
        private final String source;
        private final String dest;
        WriterFactory(String source, String dest) {
            this.source = source;
            this.dest = dest;
        }

        @Override
        public ChannelProxy apply(File file) {
            File working = file;
            if (file.path().startsWith(source))
            {

                working=new File(dest, file.path().substring(source.length()));
            }
            return new ChannelProxy(working, openChannelForWrite(working));
        }
    }
    /**
     * Creates the ChannelProxyFactory.
     *
     * @param source The source directory
     * @param dest The destination directory
     */
    protected RelocatingChannelProxyFactory(String source, String dest) {
        super(new ReaderFactory(source, dest), new WriterFactory(source, dest));
    }

    // for ParameterizedClass use
    public RelocatingChannelProxyFactory(Map<String,String> args) throws IOException
    {
        this(args.get("source"), args.get("dest"));
    }

}
