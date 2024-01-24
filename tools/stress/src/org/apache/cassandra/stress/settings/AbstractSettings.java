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


import com.datastax.driver.core.BatchStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Option;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;


abstract class AbstractSettings {

    static {
        init();
    }
    static void init() {
        TypeHandler.register(DistributionFactory.class, s->OptionDistribution.get(s));
        TypeHandler.register(PartitionGenerator.Order.class, ORDER_CONVERTER);
        TypeHandler.register(DelegateFactory.class, DELEGATE_FACTORY_CONVERTER);
    }

    /* converters */

    public static final Converter<long[]> DIST_CONVERTER = s -> {
        String[] bounds = s.split("\\.\\.+");
        return new long[]{ OptionDistribution.parseLong(bounds[0]), OptionDistribution.parseLong(bounds[1])};
    };
    public static final Converter<PartitionGenerator.Order> ORDER_CONVERTER = s -> s==null?PartitionGenerator.Order.ARBITRARY: PartitionGenerator.Order.valueOf(s);

    public static final Converter<Long> DISTRIBUTION_CONVERTER = OptionDistribution::parseLong;

    /**private static final class DelegateFactory implements RatioDistributionFactory
     {
     final DistributionFactory delegate;
     final double divisor;

     private DelegateFactory(DistributionFactory delegate, double divisor)
     {
     this.delegate = delegate;
     this.divisor = divisor;
     }

     *
     */


    /* verifiers */
    public static final Predicate<String> ORDER_VERIFIER = Verifier.enumVerifier(PartitionGenerator.Order.ARBITRARY);

    /**
     * Accepts values from 0 to Integer.MSX_VALUE inclusive.
     */
    public static final Predicate<String> POSITIVE_VERIFIER = rangeVerifier(0, Range.inclusive, Integer.MAX_VALUE, Range.inclusive);

    public enum Range {inclusive, exclusive};
    /**
     * Creates a Verifier that limits integers between lower and upper bound inclusive.
     * @param lowerBound the lower bound for the test
     * @param upperBound the upper bound for the test.
     * @return the Predicate that wraps the test.
     */
    public static final Predicate<String> rangeVerifier(int lowerBound, Range lbRange,  int upperBound, Range ubRange) {
        return new Predicate<String>() {

            @Override
            public boolean test(String s) {
                int value = Integer.parseInt(s);
                boolean result = (lbRange == Range.inclusive) ? value >= lowerBound : value > lowerBound;
                return result ? (ubRange == Range.inclusive) ? value <= upperBound : value < upperBound : result;
            }
        };
    }

    public static final Predicate<String> rangeVerifier(double lowerBound, Range lbRange, double upperBound, Range ubRange) {
        return new Predicate<String>() {

            @Override
            public boolean test(String s) {
                double value = Double.parseDouble(s);
                boolean result = (lbRange == Range.inclusive) ? value >= lowerBound : value > lowerBound;
                return result ? (ubRange == Range.inclusive) ? value <= upperBound : value < upperBound : false;
            }
        };
    }

    public static String enumOptionString(final Enum<?> exemplar) {
        return String.join(", ", Arrays.stream(exemplar.getDeclaringClass().getEnumConstants())
                .map(Enum::name).collect(Collectors.toList()));
    }

    static RuntimeException asRuntimeException(Exception ex)
    {
        return ex instanceof RuntimeException ?  (RuntimeException) ex :new RuntimeException(ex);
    }

    static <T>  Map<T,Double> ratios(Converter<T> converter, String[] params) throws Exception {
        Map<T,Double> result = new HashMap<>();
        for (String param : params ) {
            String[] args = param.split("=");
            if (args.length == 2 && args[1].length() > 0 && args[0].length() > 0) {
                if (result.put(converter.apply(args[0]), Double.valueOf(args[1])) != null)
                    throw new IllegalArgumentException(args[0] + " set twice");
            }
        }
        return result;
    }

    private enum AcceptableComparators {TimeUUIDType, AsciiType, UTF8Type };
    private static final String COL_SIZE_DEFAULT="FIXED(34)";
    private static final String COL_COUNT_DEFAULT = "FIXED(5)";
    private static final String COMMAND_ADD_DEFAULT = "FIXED(1)";
    private static final String COMMAND_CLUSTERING_DEFAULT = "GAUSSIAN(1..10)";

    private static final String INSERT_VISITS_DEFAULT = "FIXED(1)";

    private static final String INSERT_REVISIT_DEFAULT = "UNIVORM(1..1M)";

    public enum TruncateWhen
    {
        NEVER, ONCE, ALWAYS
    }
    enum StressOption {
        COL_SUPER(()->0, Option.builder("col-super").hasArg().type(Integer.class).desc("Number of super columns to use. (Default 0)").build()),
        COL_COMPARATOR(()->AcceptableComparators.AsciiType.name(), Option.builder("col-comparator").hasArg().verifier(Verifier.enumVerifier(AcceptableComparators.AsciiType))
                .desc(format("Column Comparator to use. Valid values are: %s (Default %s)",enumOptionString(AcceptableComparators.AsciiType), "AsciiType")).build()),

        COL_SLICE(null, new Option("col-slice", "If set, range slices will be used for reads, otherwise a names query will be")),
        COL_TIMESTAMP(null,Option.builder("col_timestamp").hasArg().verifier(Verifier.INTEGER).desc("If set, all columns will be written with the given timestamp").build()),
        COL_SIZE(()->OptionDistribution.get(COL_SIZE_DEFAULT),Option.builder("col-size").hasArg().type(DistributionFactory.class).desc(format("Cell size distribution. (Default %s)",COL_SIZE_DEFAULT)).build()),
        COL_NAMES(null, new Option("col-names", true, "Column names")),
        COL_COUNT(()->OptionDistribution.get(COL_COUNT_DEFAULT), Option.builder("col-count").hasArg().type(DistributionFactory.class).desc(format("Cell count distribution, per operation (Default %s)", COL_COUNT_DEFAULT)).build()),
        NO_WARMUP(null,new Option("no-warmup", "Do not warmup the process")),
        COUNT(null, Option.builder("n").hasArg().desc("Number of operations to perform. Number may be followd by 'm' or 'k' (e.g. 5m).").type(Long.class).converter(DISTRIBUTION_CONVERTER).required(true).build()),

        DURATION(null, Option.builder("duration").hasArg().desc("Time to run (in seconds, minutes, hours or days).").required(true).build()),

        UNCERT_ERR(()->0.2d, Option.builder("uncert-err").hasArg().desc("Run until the standard error of the mean is below this fraction. (Default 0.2)").type(Double.class)
                        .verifier(rangeVerifier(0.0, Range.exclusive, 1.0, Range.exclusive)).build()),
        UNCERT_MIN(()->30, Option.builder("uncert-min").hasArg().desc(format("Run at least this many iterations before accepting uncertainty convergence. Only valid with --%s. (Default 30)", UNCERT_ERR.key()))
                .type(Integer.class).verifier(POSITIVE_VERIFIER).build()),
        UNCERT_MAX(()->200, Option.builder("uncert-max").hasArg().desc(format("Run at least this many iterations before accepting uncertainty convergence. Only valid with --%s. (Default 200)", UNCERT_ERR.key()))
                .type(Integer.class).verifier(POSITIVE_VERIFIER).build()),

        TRUNCATE(()->TruncateWhen.NEVER, Option.builder("truncate").hasArg().desc(format("When to truncate the table Valid values are %s. (Default %s)", enumOptionString(TruncateWhen.NEVER), TruncateWhen.NEVER))
                .verifier(Verifier.enumVerifier(TruncateWhen.NEVER)).converter(TruncateWhen::valueOf).build()),

        CONSISTENCY(()->ConsistencyLevel.LOCAL_ONE, Option.builder("cl").hasArg().desc(format("Consistency level to use. Valid options are %s. (Default %s)", enumOptionString(ConsistencyLevel.LOCAL_ONE), ConsistencyLevel.LOCAL_ONE))
                .verifier(Verifier.enumVerifier(ConsistencyLevel.LOCAL_ONE)).type(ConsistencyLevel.class).converter(ConsistencyLevel::valueOf).build()),

        COMMAND_ADD(()->OptionDistribution.get(COMMAND_ADD_DEFAULT), Option.builder("command-add").hasArg().type(DistributionFactory.class).desc(format("Distribution of value of counter increments. (Default %s)", COMMAND_ADD_DEFAULT)).build()),
        COMMAND_KEYSIZE(()->10, Option.builder("comamnd-keysize").hasArg().desc("Key size in bytes. (Default 10)").type(Integer.class)
                .verifier(POSITIVE_VERIFIER).build()),


        COMMAND_CLUSTERING(()->OptionDistribution.get(COMMAND_CLUSTERING_DEFAULT), Option.builder("command-clustering").hasArg().desc(format("Distribution clustering runs of operations of the same kind. (Default %s)", COMMAND_CLUSTERING_DEFAULT))
                .type(DistributionFactory.class).build()),
        COMMAND_RATIO(null, Option.builder("command-ratio").hasArgs().desc("Specify the ratios for operations to perform. (e.g. (read=2 write=1) will perform 2 reads for each write)").build()),

        COMMAND_PROFILE(null, Option.builder("command-profile").desc("Specify the path to a yaml cql3 profile. Multiple comma separated files can be added.").hasArgs().required().build()),

        CREDENTIAL_FILE(null, Option.builder("credential-file").hasArg().argName("file").desc(format("File is supposed to be a standard property file with '%s', '%s', '%s', '%s', '%s', and '%s' as keys. " +
                "The values for these keys will be overriden by their command-line counterparts when specified.%n",
                SettingsCredentials.CQL_USERNAME_PROPERTY_KEY,
                SettingsCredentials.CQL_PASSWORD_PROPERTY_KEY,
                SettingsCredentials.JMX_USERNAME_PROPERTY_KEY,
                SettingsCredentials.JMX_PASSWORD_PROPERTY_KEY,
                SettingsCredentials.TRANSPORT_KEYSTORE_PASSWORD_PROPERTY_KEY,
                SettingsCredentials.TRANSPORT_TRUSTSTORE_PASSWORD_PROPERTY_KEY)).build()),

        ERROR_RETRIES(()->9, Option.builder("retries").hasArg().type(Integer.class).verifier(POSITIVE_VERIFIER).desc("Number of tries to perform for each operation before failing.").build()),
        ERROR_IGNORE(null, new Option("error-ignore", "Do not fail on errors.")),
        SKIP_READ_VALIDATION(null, new Option("skip-read-validation", "Skip read validation and message output.")),

        GRAPH_FILE(null, Option.builder("grpah-file").required().hasArg().desc("HTML file to create or append to.").build()),
        GRAPH_REVISION(null, new Option("graph-revision", true, "Unique name to assign to the current configuration being stressed.")),
        GRAPH_TITLE(null, new Option("graph-title", true, "Title for chart. (Default: current date)")),
        GRAPH_NAME(null, new Option("graph-name", true, "Alternative name for current operation (Default: stress op name)")),

/*
        final OptionDistribution visits = new OptionDistribution("visits=", "fixed(1)", "The target number of inserts to split a partition into; if more than one, the partition will be placed in the revisit set");
        final OptionDistribution revisit = new OptionDistribution("revisit=", "uniform(1..1M)", "The distribution with which we revisit partial writes (see visits); implicitly defines size of revisit collection");
        final OptionDistribution partitions = new OptionDistribution("partitions=", null, "The number of partitions to update in a single batch", false);
        final OptionSimple batchType = new OptionSimple("batchtype=", "unlogged|logged|counter", null, "Specify the type of batch statement (LOGGED, UNLOGGED or COUNTER)", false);
        final OptionRatioDistribution selectRatio = new OptionRatioDistribution("select-ratio=", null, "The uniform probability of visiting any CQL row in the generated partition", false);
        final OptionRatioDistribution rowPopulationRatio = new OptionRatioDistribution("row-population-ratio=", "fixed(1)/1", "The percent of a given rows columns to populate", false);

 */
        INSERT_VISITS(()->OptionDistribution.get(INSERT_VISITS_DEFAULT), Option.builder("insert-visits").hasArg().type(DistributionFactory.class)
        .desc(format("The target number of inserts to split a partition into; if more than one, the partition will be placed in the revisit set. (Default %s)", INSERT_VISITS_DEFAULT)).build()),
        INSERT_REVISIT(()->OptionDistribution.get(INSERT_REVISIT_DEFAULT), Option.builder("insert-revisit").hasArg().type(DistributionFactory.class)
                .desc(format("The distribution with which we revisit partial writes (see %s); implicitly defines size of revisit collection. (Default %s)", INSERT_VISITS.key(), INSERT_REVISIT_DEFAULT)).build()),
        INSERT_PARTITIONS(null, Option.builder("insert-partitions").hasArg().type(Integer.class).verifier(POSITIVE_VERIFIER).desc("The number of partitions to update in a single batch").build()),

        INSERT_BATCH_TYPE(null, Option.builder("insert-batchtype").hasArg().verifier(Verifier.enumVerifier(BatchStatement.Type.UNLOGGED))
                .converter(s ->BatchStatement.Type.valueOf(s.toUpperCase())).desc(format("Specify the type of batch statement. Valid values are %s.", enumOptionString(BatchStatement.type.UNLOGGED))).build()),

        INSERT_SELECT_RATIO(null, Option.builder("insert-select-ratio"))
        ;

        private Option option;
        private Supplier<?> dfltSupplier;
        StressOption(Supplier<?> dfltSupplier, Option option) {
            this.dfltSupplier = dfltSupplier;
            this.option = option;
        }

        public Option option() {
            return option;
        }

        public <T> Supplier<T> dfltSupplier() {
            return dfltSupplier == null ? ()->null : (Supplier<T>) dfltSupplier;
        }

        public String key() {
            return option.getOpt();
        }
    }

    public static Options allOptions() {
        Options result = new Options();
        for (StressOption so : StressOption.values()) {
            result.addOption(so.option());
        }
        return result;
    }
}
