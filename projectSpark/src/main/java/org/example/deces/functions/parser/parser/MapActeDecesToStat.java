package org.example.deces.functions.parser.parser;

import org.apache.spark.api.java.function.MapFunction;
import org.example.deces.functions.parser.beans.ActeDeces;
import org.example.deces.functions.parser.beans.Stat;


public class MapActeDecesToStat implements MapFunction<ActeDeces, Stat> {

    @Override
    public Stat call(ActeDeces acteDeces) throws Exception {
        return Stat
                .builder()
                .sexe(acteDeces.getSexe())
                .count(1)
                .build();
    }

}
