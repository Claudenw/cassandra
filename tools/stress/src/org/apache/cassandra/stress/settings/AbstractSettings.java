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


import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.commons.cli.TypeHandler;
import org.apache.commons.cli.converters.Converter;
import org.apache.commons.cli.converters.Verifier;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

abstract class AbstractSettings {

    static {
        init();
    }
    static void init() {
        TypeHandler.register(DistributionFactory.class, s->OptionDistribution.get(s), null);
        TypeHandler.register(PartitionGenerator.Order.class, ORDER_CONVERTER, ORDER_VERIFIER);
    }

    public static final Converter<long[]> DIST_CONVERTER = s -> {
        String[] bounds = s.split("\\.\\.+");
        return new long[]{ OptionDistribution.parseLong(bounds[0]), OptionDistribution.parseLong(bounds[1])};
    };

    public static final Verifier ORDER_VERIFIER = s -> {
        try {
            if (s != null){
                PartitionGenerator.Order.valueOf(s);
            }
            return true;
        } catch (NoSuchElementException e) {
            return false;
        }
    };

    public static final Converter<PartitionGenerator.Order> ORDER_CONVERTER = s -> s==null?PartitionGenerator.Order.ARBITRARY: PartitionGenerator.Order.valueOf(s);

    /**
     * A verifier driven by the values in an enum.
     */
    public static class EnumVerifier implements Verifier {
        /** The list of valid names */
        private List<String> names;

        /**
         * Constructs the Verifier from an exemplar of the Enum. For example
         * {@code new EnumVerifier(java.time.format.TextStyle.FULL)} would create an
         * Verifier that would accept the names for any of the
         * {@code java.time.format.TextStyle} values.
         * @param exemplar One of the values from the accepted Enum.
         */
        public EnumVerifier(Enum<?> exemplar) {
            names = Arrays.stream(exemplar.getDeclaringClass().getEnumConstants()).map(t -> ((Enum<?>) t).name())
                          .collect(Collectors.toList());
        }

        @Override
        public boolean test(String str) throws RuntimeException {
            return names.contains(str);
        }
    }

    static RuntimeException asRuntimeException(Exception ex)
    {
        return ex instanceof RuntimeException ?  (RuntimeException) ex :new RuntimeException(ex);
    }
}
