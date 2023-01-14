package org.example.deces.functions.parser.parser;

import org.example.deces.functions.parser.beans.Stat;

public class ReduceFunction implements org.apache.spark.api.java.function.ReduceFunction<Stat> {
    @Override
    public Stat call(Stat stats, Stat t1) throws Exception {

        return Stat.builder()
                .count(stats.getCount() + stats.getCount())
                .build();
    }
}
