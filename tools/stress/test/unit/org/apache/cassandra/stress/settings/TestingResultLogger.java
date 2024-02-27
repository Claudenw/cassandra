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

package org.apache.cassandra.stress.settings;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.cassandra.stress.util.ResultLogger;

import static org.junit.Assert.assertTrue;

public class TestingResultLogger implements ResultLogger
{
    List<String> results = new ArrayList<>();

    public void assertEndsWith(String s) {
        assertTrue(String.format("Missing '%s'", s), results.stream().filter( str -> str.endsWith(s) ).findFirst().isPresent());
    }

    public void assertEndsWith(String s, Supplier<String> msg) {
        assertTrue(String.format("%s: Missing '%s'", msg.get(), s), results.stream().filter( str -> str.endsWith(s) ).findFirst().isPresent());
    }

    public void assertContainsRegex(String regex) {
        assertContains(Pattern.compile(regex).asPredicate(), ()->"Missing: "+regex);
    }

    private void assertContains(Predicate<String> predicate, Supplier<String> msg) {
        assertTrue(msg.get(), results.stream().filter( predicate ).findFirst().isPresent());
    }
    @Override
    public void println(String line)
    {
        results.add(line);
    }

    @Override
    public void println()
    {
    }

    @Override
    public void printException(Exception e)
    {
        println(e.toString());
    }

    @Override
    public void flush()
    {

    }

    @Override
    public void printf(String s, Object... args)
    {
        println(String.format(s, args));
    }
}
