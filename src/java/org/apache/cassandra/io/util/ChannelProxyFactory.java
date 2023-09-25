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

import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.SyncUtil;

public class ChannelProxyFactory {

    private static final Logger logger = LoggerFactory.getLogger(ChannelProxyFactory.class);

    private static ChannelProxyFactory INSTANCE;

    public static ChannelProxy forReading(File file) {
        return file == null? null : ChannelProxyFactory.instance().reader().apply(file);
    }

    public static ChannelProxy forWriting(File file) {
        return file == null ? null : ChannelProxyFactory.instance().writer().apply(file);
    }

    /**
     * Creates a ProxyFactory that matches the defaults for read and write for version 4.x
     * @return The default ChannelProxyFactory.
     */
    private static ChannelProxyFactory createDefault() {
        Config conf = DatabaseDescriptor.getRawConfig();
        if (conf.channel_proxy_factory == null) {
            return new ChannelProxyFactory( ChannelProxy::new, f -> new ChannelProxy(f, openChannelForWrite(f)));
        } else {
            return ParameterizedClass.newInstance(conf.channel_proxy_factory, Collections.emptyList());
        }
    }

    /**
     * Creates a FileChannel for writing on the file.  Will create the  file if it does not exist.
     * @param file the File to check.
     * @return the FileChannel for the file.
     */
    protected static FileChannel openChannelForWrite(File file)
    {
        if (file.exists()) {
            return ChannelProxy.openChannel(file, StandardOpenOption.WRITE);
        } else {
            FileChannel channel = ChannelProxy.openChannel(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            try {
                SyncUtil.trySyncDir(file.parent());
            } catch (Throwable t) {
                try {
                    channel.close();
                } catch (Throwable t2) {
                    t.addSuppressed(t2);
                }
            }
            return channel;
        }
    }

    /**
     * Returns the registered ChannelProxyFactory.  Will create the default if no instance has been set.
     * @return The registered ChannelProxyFactory.
     */
    public static ChannelProxyFactory instance() {
        ChannelProxyFactory result = INSTANCE;
        if (result == null) {
            INSTANCE = result = createDefault();
            logger.warn( "Created ChannelProxyFactory of class: {}", result.getClass().getName());
        }
        return result;
    }

    /**
     * Sets the instance of the ChannelProxyFactory.
     * @param instance the instance to use.
     */
    public static void setInstance(ChannelProxyFactory instance) {
        INSTANCE = instance;
    }

    /**
     * The function to convert a File to a ChannelProxy for reading..
     */
    protected Function<File, ChannelProxy> readerFactory;
    /**
     * The function to convert a File to a ChannelProxy for writing.
     */
    protected Function<File, ChannelProxy> writerFactory;

    /**
     * Creates the ChannelProxyFactory.
     * @param readerFactory The function to create a ChannelProxy for reading from a File.
     * @param writerFactory The function to create a ChannelProxy for writing from a File.
     */
    protected ChannelProxyFactory(Function<File, ChannelProxy> readerFactory, Function<File, ChannelProxy> writerFactory) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
    }

    /**
     * Gets the reader function.
     * @return the function to create a ChannelProxy for reading from a File.
     */
    public Function<File, ChannelProxy> reader() {
        return readerFactory;
    }

    /**
     * Gets the writer function.
     * @return the function to create a ChannelProxy for writing from a File.
     */
    public Function<File, ChannelProxy> writer() {
        return writerFactory;
    }
}
