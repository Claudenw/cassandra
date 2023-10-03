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
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;

import org.apache.cassandra.io.sstable.*;

import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileSystemMapperTest extends CQLTester
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
        FileSystemMapper.setInstance(null);
        new File(directory.toFile()).deleteRecursive();
        new File(otherDirectory.toFile()).deleteRecursive();
        DatabaseDescriptor.getRawConfig().file_system_mapper_handler = null;
    }
    @Test
    public void testConfigProxyReadWrite() throws Exception
    {
        Map<String,String> args = new HashMap<>();
        args.put("from", directory.toString());
        args.put("to", otherDirectory.toString());
        DatabaseDescriptor.getRawConfig().file_system_mapper_handler = new ParameterizedClass(TestFactory.class.getName(), args);
        executeReadWriteTest();
    }


    @Test
    public void testDefaultProxyReadWrite() throws Exception
    {
        executeReadWriteTest();
    }

    @Test
    public void testCustomProxyReadWrite() throws Exception {
        FileSystemMapper.setInstance(null);
        FileSystemMapper.addHandler( new TestFactory(directory.toString(), otherDirectory.toString()));
        executeReadWriteTest();
    }

    private void executeReadWriteTest() throws IOException {
        File file =  descriptor.fileFor(SSTableFormat.Components.DATA);

        assertTrue( file.path().startsWith(otherDirectory.toString()));

        try (SequentialWriter writer = new SequentialWriter(file, SequentialWriterOption.FINISH_ON_CLOSE)) {
            writer.writeBytes("hello world");
        }

        FileHandle fh =  new FileHandle.Builder(file).complete();
        ByteBuffer expected = ByteBuffer.wrap( "hello world".getBytes());
        ByteBuffer read = ByteBuffer.allocate( 100 );

        int len = fh.channel.read(read,0);
        assertEquals( expected.capacity(), len );
        read.flip();
        assertEquals( expected, read );
    }


    public static class TestFactory implements FileSystemMapperHandler {

        private String from;
        private String to;
        public TestFactory(String from, String to) throws IOException {
            this.from = from;
            this.to = to;
        }

        @Override
        public Path getPath(String first, String... more)
        {
            FileSystem fs = FileSystems.getDefault();
            String pathStr = fs.getPath(first,more).toString().toString();
            return pathStr.startsWith(from.toString()) ? fs.getPath(to, pathStr.substring(from.length())) : null;
        }
    }
}
