/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.util;


import org.junit.Assert;

import java.io.IOException;

public class ChecksummedSequentialWriterProxyFactoryTest extends SequentialWriterProxyFactoryTest
{

    protected TestableTransaction newTest() throws IOException
    {
        TestableCSW sw = new TestableCSW();
        writers.add(sw);
        return sw;
    }

    private static class TestableCSW extends TestableSW
    {
        final ChannelProxy crcProxy;
        private TestableCSW() throws IOException
        {
            this(tempFile("compressedsequentialwriter"),
                 tempFile("compressedsequentialwriter.checksum"));
        }

        private TestableCSW(File file, File crcFile) throws IOException
        {
            this(getProxy(file), getProxy(crcFile));
        }

        private TestableCSW(ChannelProxy proxy, ChannelProxy crcProxy) throws IOException
        {
            this(proxy, crcProxy, new ChecksummedSequentialWriter(proxy, crcProxy, null, SequentialWriterOption.newBuilder()
                    .bufferSize(BUFFER_SIZE)
                    .build()));
        }
        private TestableCSW(ChannelProxy proxy, ChannelProxy crcProxy, SequentialWriter sw) throws IOException
        {
            super(proxy, sw);
            this.crcProxy = crcProxy;
        }

        protected void assertInProgress() throws Exception
        {
            super.assertInProgress();
            File crcFile = proxyMap.get(crcProxy);
            Assert.assertTrue(crcFile.exists());
            Assert.assertEquals(0, crcFile.length());
        }

        protected void assertPrepared() throws Exception
        {
            super.assertPrepared();
            File crcFile = proxyMap.get(crcProxy);
            Assert.assertTrue(crcFile.exists());
            Assert.assertFalse(0 == crcFile.length());
        }

        protected void assertAborted() throws Exception
        {
            super.assertAborted();
        }
    }
}
