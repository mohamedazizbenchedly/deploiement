package org.example.deces.functions.parser.parser;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.deces.functions.parser.beans.ActeDeces;

import java.util.function.Function;

public class ActeDecesMapper implements Function<Dataset<Row>, Dataset<ActeDeces>>  {
    private final RowToActeDecesFunc parser = new RowToActeDecesFunc();

    //fct spark
    // en lambda : task =  r -> parser.apply() ;
    private final MapFunction<Row, ActeDeces> task = parser::apply;

    @Override
    public Dataset<ActeDeces> apply(Dataset<Row> inputDS) {
        return inputDS.map(task, Encoders.bean(ActeDeces.class));

    }
}
