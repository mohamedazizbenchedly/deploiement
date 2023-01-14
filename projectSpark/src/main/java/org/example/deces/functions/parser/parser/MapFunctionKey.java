package org.example.deces.functions.parser.parser;

import org.apache.spark.api.java.function.MapFunction;
import org.example.deces.functions.parser.beans.ActeDeces;

public class MapFunctionKey implements  MapFunction<ActeDeces,String>{


    @Override
    public String call(ActeDeces acteDeces) throws Exception {

        return acteDeces.getSexe();
    }
}

