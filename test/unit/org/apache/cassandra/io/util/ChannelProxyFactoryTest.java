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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;

import org.apache.cassandra.io.sstable.*;

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.utils.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ChannelProxyFactoryTest extends CQLTester
{
    private Path directory;
    private Path otherDirectory;

    private SSTableId tableId;

    private Descriptor descriptor;

    private final String ksname = "keyspace";
    private final String cfname = "table";

    @Before
    public void setup() throws IOException {
        directory = Files.createTempDirectory("proxyTest");
        otherDirectory = Files.createTempDirectory("proxyFactoryTest");
        tableId = new SequenceBasedSSTableId(1 );
        descriptor = new Descriptor(new File(directory.toFile()), ksname, cfname, tableId);
    }

    @After
    public void tearDown() throws IOException {
        new File(directory.toFile()).deleteRecursive();
        new File(otherDirectory.toFile()).deleteRecursive();
        ChannelProxyFactory.setInstance(null);
        DatabaseDescriptor.getRawConfig().channel_proxy_factory = null;
    }

    private static class Tracker implements LifecycleNewTracker {

        @Override
        public void trackNew(SSTable table)
        {
            System.out.println( "tracking: "+table);
        }

        @Override
        public void untrackNew(SSTable table)
        {
            System.out.println( "untracking tracking: "+table);
        }

        @Override
        public OperationType opType()
        {
            return null;
        }
    }

    @Test
    public void testConfigProxyReadWrite() throws Exception
    {
        Map<String,String> args = new HashMap<>();
        args.put("directory", otherDirectory.toString());
        DatabaseDescriptor.getRawConfig().channel_proxy_factory = new ParameterizedClass(TestFactory.class.getName(), args);
        executeReadWriteTest();
    }


    @Test
    public void testDefaultProxyReadWrite() throws Exception
    {
        executeReadWriteTest();
    }

    @Test
    public void testCustomProxyReadWrite() throws Exception {
        TestFactory testFactory = new TestFactory(otherDirectory);
        ChannelProxyFactory.setInstance(testFactory);
        executeReadWriteTest();
    }

    private void executeReadWriteTest() throws IOException {
        ChannelProxy proxy = org.apache.cassandra.io.util.ChannelProxyFactory.instance().writer().apply(descriptor.fileFor(SSTableFormat.Components.DATA));

        try (SequentialWriter writer = new SequentialWriter(proxy, SequentialWriterOption.FINISH_ON_CLOSE)) {
            writer.writeBytes("hello world");
        }
        assertFalse( proxy.channel().isOpen());
        proxy = org.apache.cassandra.io.util.ChannelProxyFactory.instance().reader().apply(descriptor.fileFor(SSTableFormat.Components.DATA));
        ByteBuffer expected = ByteBuffer.wrap( "hello world".getBytes());
        ByteBuffer read = ByteBuffer.allocate( 100 );
        int len = proxy.channel().read(read);
        assertEquals( expected.capacity(), len );
        read.flip();
        assertEquals( expected, read );
    }


    public static class TestFactory extends ChannelProxyFactory {

        public TestFactory(Map<String,String> args) throws IOException {
            this(Path.of(args.get("directory")));
        }

        TestFactory(Path directory) throws IOException {
            super(createFactory(directory, StandardOpenOption.READ), createFactory(directory,StandardOpenOption.WRITE));
        }

        static Function<File, ChannelProxy> createFactory(Path directory, StandardOpenOption opt) {
            return f -> {
                try {
                    Pair<Descriptor, Component> pair = Descriptor.fromFileWithComponent(f, true);
                    Descriptor newDescriptor = new Descriptor(new File(directory.toFile()), pair.left.ksname, pair.left.cfname, pair.left.id);
                    if (opt == StandardOpenOption.READ) {
                        return new ChannelProxy(f, ChannelProxy.openChannel(newDescriptor.fileFor(pair.right), opt));
                    } else {
                        return new ChannelProxy(f, ChannelProxyFactory.openChannelForWrite(newDescriptor.fileFor(pair.right)));
                    }
                } catch (IllegalArgumentException e) {
                    File source = new File( directory.resolve(f.path()).toFile());
                    if (opt == StandardOpenOption.READ) {
                        return new ChannelProxy(f, ChannelProxy.openChannel(source, opt));
                    } else {
                        return new ChannelProxy(f, ChannelProxyFactory.openChannelForWrite(source));
                    }

                }

            };
        }
    }
}
