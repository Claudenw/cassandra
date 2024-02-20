package org.apache.cassandra.stress.settings;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;

import com.google.common.base.Function;

import org.apache.commons.cli.Options;

import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.RatioDistribution;
import org.apache.cassandra.stress.generate.RatioDistributionFactory;

/**
 * For selecting a mathematical distribution
 */
public class OptionRatioDistribution
{

    public static final Function<String, RatioDistributionFactory> BUILDER = new Function<String, RatioDistributionFactory>()
    {
        public RatioDistributionFactory apply(String s)
        {
            return get(s);
        }
    };

    private static final Pattern FULL = Pattern.compile("(.*)/([0-9]+[KMB]?)", Pattern.CASE_INSENSITIVE);




//    @Override
//    public boolean accept(String param)
//    {
//        Matcher m = FULL.matcher(param);
//        if (!m.matches() || !delegate.accept(m.group(1)))
//            return false;
//        divisor = OptionDistribution.parseLong(m.group(2));
//        return true;
//    }

    public static RatioDistributionFactory get(String spec)
    {
        Matcher m = FULL.matcher(spec);
        if (m.matches())
        {
            final DistributionFactory factory = OptionDistribution.get(m.group(1));
            final double divisor = OptionDistribution.parseLong(m.group(2));
            return new DelegateFactory(factory, divisor);
        }
        throw new IllegalArgumentException("Invalid ratio definition: "+spec);
    }

//    public RatioDistributionFactory get()
//    {
//        if (delegate.setByUser())
//            return new DelegateFactory(delegate.get(), divisor);
//        if (defaultSpec == null)
//            return null;
//        OptionRatioDistribution sub = new OptionRatioDistribution("", null, null, true);
//        if (!sub.accept(defaultSpec))
//            throw new IllegalStateException("Invalid default spec: " + defaultSpec);
//        return sub.get();
//    }

    public static Options argumentOptions() {
        Options result = OptionDistribution.argumentOptions();
        result.getOptions().forEach( o -> {
            o.setLongOpt(o.getLongOpt()+"/divisor");
        });
        return result;
    }

    public static List<String> argumentNotes() {
        List<String> notes = new ArrayList<>();
        notes.add( "All values are calculated as specified above and then divided by the divisor.");
        notes.addAll(OptionDistribution.argumentNotes());
        return notes;
    }


    // factories

    public static final class DelegateFactory implements RatioDistributionFactory
    {
        final DistributionFactory delegate;
        final double divisor;

        private DelegateFactory(DistributionFactory delegate, double divisor)
        {
            this.delegate = delegate;
            this.divisor = divisor;
        }

        @Override
        public RatioDistribution get()
        {
            return new RatioDistribution(delegate.get(), divisor);
        }

        @Override
        public String getConfigAsString(){return String.format("Ratio: divisor=%f;delegate=%s",divisor, delegate.getConfigAsString());};

    }

}
