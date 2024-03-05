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


import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.generate.RatioDistributionFactory;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Option;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;

import static java.lang.String.format;


public abstract class AbstractSettings implements Serializable
{

    /* converters */

    public static final Converter<long[], PatternSyntaxException> DIST_CONVERTER = s -> {
        String[] bounds = s.split("\\.\\.+");
        return new long[]{ OptionDistribution.parseLong(bounds[0]), OptionDistribution.parseLong(bounds[1])};
    };
    public static final Converter<PartitionGenerator.Order, IllegalArgumentException> ORDER_CONVERTER = s -> s==null?PartitionGenerator.Order.ARBITRARY: PartitionGenerator.Order.valueOf(s);

    public static final Converter<Long,NumberFormatException> DISTRIBUTION_CONVERTER = OptionDistribution::parseLong;

    public static final Converter<Integer,NumberFormatException> TOKENRANGE_CONVERTER = s -> Ints.checkedCast(OptionDistribution.parseLong(s));

    public static final Converter<RatioDistributionFactory,Exception>  RATIO_DISTRIBUTION_FACTORY_CONVERTER = OptionRatioDistribution::get;

    public static final Converter<DurationSpec.IntSecondsBound,Exception> DURATION_CONVERTER = s -> new DurationSpec.IntSecondsBound(s);

    public static final Converter<DistributionFactory,Exception> DISTRIBUTION_FACTORY_CONVERTER =  OptionDistribution::get;

    public static Map<String,StressArgument> argumentMap;

    static {
        initializeArguments();
    }

    static void initializeArguments() {
        argumentMap =  new TreeMap<>();

        StressArgument sa = new StressArgument("distribution", DistributionFactory.class, DISTRIBUTION_FACTORY_CONVERTER);
        OptionDistribution.argumentOptions().getOptions().forEach(sa.options::addOption);
        sa.notes.addAll(OptionDistribution.argumentNotes());

        sa = new StressArgument("distribution_ratio", RatioDistributionFactory.class, RATIO_DISTRIBUTION_FACTORY_CONVERTER);
        OptionRatioDistribution.argumentOptions().getOptions().forEach(sa.options::addOption);
        sa.notes.addAll(OptionRatioDistribution.argumentNotes());

        sa = new StressArgument("duration", DurationSpec.IntSecondsBound.class, DURATION_CONVERTER);
        sa.notes.add("Duration followed by unit pattern of d, h, m, s, ms, us, µs, or ns. (e.g. 25m = 25 minutes)");

        sa = new StressArgument("file", java.io.File.class, Converter.FILE);

        sa = new StressArgument("partition_order", PartitionGenerator.Order.class, ORDER_CONVERTER);
        sa.notes.add("Valid options are: "+enumOptionString(PartitionGenerator.Order.ARBITRARY));

        sa = new StressArgument("ratio", null, null);
        sa.notes.add("Ratios are specified by listing the operation number of times it should be executed in the form 'operation=rate'." );
        sa.notes.add("For example the patterns 'read=2' and 'write=1' will perform 2 reads for each write.");
        sa.notes.add("You may specify user defined queries by name.  For example 'insert=2' and 'query1=1' will perform 2 inserts for each query1, where 'query1' is defined in the yaml file.");
    }





    /* verifiers */


    /**
     * Constructs a {@code Predicate<String>} from an exemplar of an Enum. For
     * example {@code new EnumVerifier(java.time.format.TextStyle.FULL)} would
     * create an {@code Predicate<String>} that would accept the names for any of
     * the {@code java.time.format.TextStyle} values.
     * @param exemplar One of the values from the accepted Enum.
     * @return A {@code Predicate<String>} that matches the Enum names.
     */
    public static Predicate<String> enumVerifier(final Enum<?> exemplar) {
        return new Predicate<>() {
            /** The list of valid names */
            private final List<String> names = Arrays.stream(exemplar.getDeclaringClass().getEnumConstants())
                    .map(Enum::name).collect(Collectors.toList());
            @Override
            public boolean test(final String str) throws RuntimeException {
                return names.contains(str);
            }
        };
    }

    public static <T>  Predicate<T> isNullOr(Predicate<T> p)
    {
        Predicate<T> outer = s -> Objects.isNull(s);
        return outer.or(p);
    }

    /**
     * Accepts values from 0 to Long.MAX_VALUE inclusive
     */
    public static final Predicate<Long> LONG_POSITIVE_VERIFIER = rangeVerifier(0, Range.inclusive, Long.MAX_VALUE, Range.inclusive);
    public static Predicate<Long> rangeVerifier(long lowerBound, Range lbRange,  long upperBound, Range ubRange) {
        return value -> {
            boolean result = (lbRange == Range.inclusive) ? value >= lowerBound : value > lowerBound;
            return result && ((ubRange == Range.inclusive) ? value <= upperBound : value < upperBound);
        };
    }
    /**
     * Accepts values from 0 to Integer.MAX_VALUE inclusive.
     */
    public static final Predicate<Integer> POSITIVE_VERIFIER = rangeVerifier(0, Range.inclusive, Integer.MAX_VALUE, Range.inclusive);

    public enum Range {inclusive, exclusive}
    /**
     * Creates a Verifier that limits integers between lower and upper bound inclusive.
     * @param lowerBound the lower bound for the test
     * @param upperBound the upper bound for the test.
     * @return the Predicate that wraps the test.
     */
    public static Predicate<Integer> rangeVerifier(int lowerBound, Range lbRange,  int upperBound, Range ubRange) {
        return value -> {
                boolean result = (lbRange == Range.inclusive) ? value >= lowerBound : value > lowerBound;
                return result && ((ubRange == Range.inclusive) ? value <= upperBound : value < upperBound);
        };
    }

    public static Predicate<Double> rangeVerifier(double lowerBound, Range lbRange, double upperBound, Range ubRange) {
        return value -> {
                boolean result = (lbRange == Range.inclusive) ? value >= lowerBound : value > lowerBound;
                return result && ((ubRange == Range.inclusive) ? value <= upperBound : value < upperBound);
        };
    }

    public static final Predicate<Integer> portBoundsChecker = rangeVerifier(0, Range.inclusive, 65535, Range.inclusive);
    public static String enumOptionString(final Enum<?> exemplar) {
        return String.join(", ", Arrays.stream(exemplar.getDeclaringClass().getEnumConstants())
                .map(Enum::name).collect(Collectors.toList()));
    }

    public static RuntimeException asRuntimeException(Exception ex)
    {
        return ex instanceof RuntimeException ?  (RuntimeException) ex :new RuntimeException(ex);
    }

    public static class StressOption<T> {

        private Option option;
        private Supplier<T> dfltSupplier;

        private Predicate<T> verifier;

        StressOption(Option option) {
            this(null, null, option);
        }

        StressOption(Supplier<T> dfltSupplier, Option option) {
            this(dfltSupplier,null,option);
        }

        StressOption(Predicate<T> verifier, Option option) {
            this(null, verifier, option);
        }
        StressOption(Supplier<T> dfltSupplier, Predicate<T> verifier, Option option) {
            this.dfltSupplier = dfltSupplier;
            this.verifier = verifier;
            this.option = option;
            if (option.getArgName() == null) {
                Optional<StressArgument> sa = argumentMap.values().stream().filter(s -> s.resultType == option.getType()).findFirst();
                if (sa.isPresent())
                    option.setArgName(sa.get().name);
            }
        }

        public Option option() {
            return option;
        }

        public Supplier<T> dfltSupplier() {
            return dfltSupplier == null ? ()->null : dfltSupplier;
        }

        public String key() {
            return format("-%s",option.getOpt());
        }

        public T extract(CommandLine commandLine, T defaultValue)
        {
            T result = extract(commandLine);
            return result == null ? defaultValue : result;
        }

        /**
         * Returns the obtion as the parsed value or the default if provided.
         * Resulting object must pass the verifier (if any).
         * @param commandLine the command line to process
         * @return an instance of the class defined for the option.
         * @see CommandLine#getParsedOptionValue(Option, Supplier)
         * @throws RuntimeException wrapping any non-runtime execption.
         */
        public T extract(CommandLine commandLine)
        {
            try
            {
                T value = commandLine.getParsedOptionValue(option(), dfltSupplier);
                if (verifier != null && !verifier.test(value)) {
                    throw new ParseException(format("'%s' is not a legal value for %s", commandLine.getOptionValue(option), key()));
                }
                return value;
            } catch (Exception e) {
                throw asRuntimeException(e);
            }
        }

        /**
         * Returns the option values as an array of String.
         * @param commandLine the command line for input
         * @return an array of items from this option.  May return null.
         */
        public String[] extractArray(CommandLine commandLine) {
            String[] values = commandLine.getOptionValues(this.option);
            return  values == null ? (String[]) dfltSupplier().get() : values;
        }

        /**
         * Returns a map values interpreted as property values.
         * @param commandLine the command line for input
         * @return a Map based on the properties from the command line.
         * @see CommandLine#getOptionProperties(Option)
         */
        public Map<String,String> extractMap(CommandLine commandLine)
        {
            Map<String,String> result = extractMap(commandLine, k->k, v->v);
            return result;
        }

        /**
         * Returns a map values interpreted as property values converted by the key and value converters.
         * @param commandLine the command line for input
         * @param keyConvert the converter for the input string to the key value.
         * @param valueConvert the converter for the input string to the value converter.
         * @return a Map based on the properties from the command line.
         * @see CommandLine#getOptionProperties(Option)
         */
        public <K,V> Map<K,V> extractMap(CommandLine commandLine, Converter<K, Exception> keyConvert, Converter<V, Exception> valueConvert)
        {
            Map<K,V> result = new HashMap<>();
            Properties p = commandLine.getOptionProperties(option);
            for (Map.Entry<Object,Object> entry : p.entrySet())
            {
                try
                {
                    K key = keyConvert.apply(entry.getKey().toString());
                    if (result.containsKey(key)) {
                        throw new IllegalArgumentException(key + " set twice");
                    }
                    result.put(key,valueConvert.apply(entry.getValue().toString()));
                }
                catch (Exception e)
                {
                    throw asRuntimeException(e);
                }
            }
            return result;
        }
    }

    /**
     * Definitions for various argument types based on the name of the argument.
     * @param <T>
     */
    public static class StressArgument<T>
    {
        /**
         * The name of the argument.
         */
        final String name;
        /**
         * The result type for the argument
         */
        final Class<T> resultType;
        /**
         * The converter that converts from a string to the Argument.
         */
        final Converter<T,?> converter;
        /**
         * Options used to print the argument and notes.
         */
        final Options options;
        /**
         * The list of strings that comprise the notes for the argument type.
         */
        final List<String> notes;

        StressArgument(String name, Class<T> resultType, Converter<T,?> converter ) {
            this.name = name;
            this.resultType = resultType;
            this.converter = converter;
            this.options = new Options();
            this.notes = new ArrayList<>();
            argumentMap.put(name, this);
            TypeHandler.register(this.resultType, this.converter);
        }

        public void addOption( String name, String description) {
            options.addOption( Option.builder(null).longOpt(name).desc(description).build());
        }
    }

    public static class PrintUtils {
        public static String printSensitive(Object sensitive) {
            return sensitive == null ? "*not set*" : "*suppressed*";
        }

        public static Object printNull(Object value) {
            return value == null ? "*not set*" : value;
        }
    }

    /**
     * An option group with a simplified message when group option is not selected or too many are selected.
     */
    public static class SimpleOptionGroup extends OptionGroup
    {
        @Override
        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            Iterator<Option> iter = this.getOptions().iterator();
            buff.append("One of the following options is required: ");

            while (iter.hasNext())
            {
                Option option = (Option) iter.next();
                if (option.getOpt() != null)
                {
                    buff.append("-");
                    buff.append(option.getOpt());
                }
                else
                {
                    buff.append("--");
                    buff.append(option.getLongOpt());
                }

                if (iter.hasNext())
                {
                    buff.append(", ");
                }
            }
            buff.append(".");
            return buff.toString();
        }
    }
}
