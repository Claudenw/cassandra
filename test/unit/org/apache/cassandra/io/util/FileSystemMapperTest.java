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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.marshal.UTF8Type;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileSystemMapperTest extends CQLTester
{
    private Path directory;

    private final String ksname = "demo";
    private final String cfname = "myTable";

    protected String createKeyspaceName()
    {
        return ksname;
    }

    @Before
    public void setup() throws IOException {
        FileSystemMapper.setInstance(null);
        execute( "drop keyspace if exists "+ksname);
        directory = Files.createTempDirectory("proxyTest");
        File f = new File("build/test/cassandra/data/"+ksname);
        if (f.exists()) {f.deleteRecursive();}
    }

    @After
    public void tearDown() throws IOException {
        FileSystemMapper.setInstance(null);
        execute( "drop keyspace if exists "+ksname);
        new File(directory.toFile()).deleteRecursive();
        DatabaseDescriptor.getRawConfig().file_system_mapper_handler = null;
        File f = new File("build/test/cassandra/data/"+ksname);
        if (f.exists()) {f.deleteRecursive();}
    }
    @Test
    public void testConfigProxyDirectories()
    {
        Map<String,String> args = new HashMap<>();
        args.put("basePath", directory.toString());
        args.put("keyspace", ksname);
        DatabaseDescriptor.getRawConfig().file_system_mapper_handler = new ParameterizedClass(RelocatingFileSystemMapper.class.getName(), args);
        executeDirectoriesTest(directory.toString(), 1);
    }


    @Test
    public void testDefaultProxyDirectories()
    {
        executeDirectoriesTest("build/test/cassandra/data", 1);
    }

    @Test
    public void testCustomProxyDirectories()
    {
        FileSystemMapper.addHandler( new RelocatingFileSystemMapper(directory.toString(), ksname, null));
        executeDirectoriesTest(directory.toString(),1);
    }

    @Test
    public void testTwoDirectories() {
        executeDirectoriesTest("build/test/cassandra/data", 1);
        FileSystemMapper.addHandler( new RelocatingFileSystemMapper(directory.toString(), ksname, null));
        executeDirectoriesTest(directory.toString(),2);
    }

    private void executeDirectoriesTest(String expectedDir, int expectedDirectoryCount)
    {
        TableMetadata metadata = TableMetadata.builder(ksname, cfname)
                                              .addPartitionKeyColumn("column1", UTF8Type.instance)
                                              .build();
        Directories directories = new Directories(metadata);
        File file = directories.getDirectoryForNewSSTables();

        assertTrue(file.path().startsWith(expectedDir));


        List<File> files = directories.getKSChildDirectories(ksname);

        files.stream().forEach(x -> logger.error(x.toString()));
        assertEquals(expectedDirectoryCount, files.size());
        assertEquals(file, files.get(0));
    }

    @Test
    public void testCQLCreation() throws IOException
    {
        Map<String, String> args = new HashMap<>();
        args.put("basePath", directory.toString());
        args.put("keyspace", ksname);
        DatabaseDescriptor.getRawConfig().file_system_mapper_handler = new ParameterizedClass(RelocatingFileSystemMapper.class.getName(), args);

        createKeyspace("create keyspace if not exists %s with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        createTable(ksname, "create table %s ( name text, comment text, PRIMARY KEY (name))", cfname);
        execute("insert into demo.myTable (name,comment) VALUES ('Claude', 'Presenting Demo' )");
        UntypedResultSet rs = execute("select * from demo.myTable");
        assertEquals(1, rs.size());
        Util.flushKeyspace(ksname);

        String[] expected = {"nc-1-big-Data.db","nc-1-big-Filter.db","nc-1-big-Statistics.db","nc-1-big-TOC.txt","nc-1-big-CompressionInfo.db","nc-1-big-Digest.crc32","nc-1-big-Index.db","nc-1-big-Summary.db"};

        File ks = new File(directory, ksname );
        File tbl = ks.tryList( File::isDirectory)[0];
        List<String> names = new ArrayList<>();
        names.addAll(Arrays.asList(tbl.toJavaIOFile().list((f,s) -> new File(f,s).isFile())));
        assertEquals( expected.length, names.size());
        names.removeAll( Arrays.asList(expected));
        assertEquals(0, names.size());
    }

    @Test
    public void testCQLCreationSplitKeyspace() throws IOException
    {
        Map<String, String> args = new HashMap<>();
        args.put("basePath", directory.toString());
        args.put("keyspace", ksname);
        args.put("tableName", cfname);
        DatabaseDescriptor.getRawConfig().file_system_mapper_handler = new ParameterizedClass(RelocatingFileSystemMapper.class.getName(), args);

        createKeyspace("create keyspace if not exists %s with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        String tableName1 = createTable(ksname, "create table %s ( name text, comment text, PRIMARY KEY (name))", cfname);
        execute(String.format("insert into demo.%s (name,comment) VALUES ('Claude', 'Presenting Demo' )", tableName1));
        UntypedResultSet rs = execute(String.format("select * from demo.%s",tableName1));
        assertEquals(1, rs.size());

        String tableName2 = createTable(ksname, "create table %s ( name text, comment text, PRIMARY KEY (name))", "myOtherTable");
        execute(String.format("insert into demo.%s (name,comment) VALUES ('Warren', 'Testing Demo' )",tableName2));
        rs = execute(String.format("select * from demo.%s", tableName2));
        assertEquals(1, rs.size());

        Util.flushKeyspace(ksname);

        String[] expected = {"nc-1-big-Data.db","nc-1-big-Filter.db","nc-1-big-Statistics.db","nc-1-big-TOC.txt","nc-1-big-CompressionInfo.db","nc-1-big-Digest.crc32","nc-1-big-Index.db","nc-1-big-Summary.db"};

        File ks = new File(directory, ksname );
        File tbl = ks.tryList( File::isDirectory)[0];
        assertTrue( tbl.path().substring( tbl.parentPath().length()+1).startsWith(tableName1.toLowerCase()));
        List<String> names = new ArrayList<>();
        names.addAll(Arrays.asList(tbl.toJavaIOFile().list((f,s) -> new File(f,s).isFile())));
        assertEquals( expected.length, names.size());
        names.removeAll( Arrays.asList(expected));
        assertEquals(0, names.size());

        ks = new File("build/test/cassandra/data/demo");
        tbl = ks.tryList( File::isDirectory)[0];
        assertTrue( tbl.path().substring( tbl.parentPath().length()+1).startsWith(tableName2.toLowerCase()));
        names.addAll(Arrays.asList(tbl.toJavaIOFile().list((f,s) -> new File(f,s).isFile())));
        assertEquals( expected.length, names.size());
        names.removeAll( Arrays.asList(expected));
        assertEquals(0, names.size());
    }
}
