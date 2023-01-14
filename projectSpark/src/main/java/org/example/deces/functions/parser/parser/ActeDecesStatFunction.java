package org.example.deces.functions.parser.parser;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import org.example.deces.functions.parser.beans.ActeDeces;
import org.example.deces.functions.parser.beans.Stat;
import scala.Tuple2;

import java.util.function.Function;


@AllArgsConstructor
@RequiredArgsConstructor
public class ActeDecesStatFunction implements Function<Dataset<Row>, Dataset<Tuple2<String, Stat>>> {



    private String key ;

    @Override
    public Dataset<Tuple2<String, Stat>> apply(Dataset<Row> rowDataset) {
        Dataset<ActeDeces> cleanDs = new ActeDecesMapper().apply(rowDataset) ;

        //Stats

        final ActeDecesToKeyFunction  acteDecesToKeyFunction = new ActeDecesToKeyFunction(key);

        final GetStatOnFunction getStatOnFunction = new GetStatOnFunction() ;

        final ReduceFunction reduceFunction = new ReduceFunction() ;


        Dataset<Tuple2<String, Stat>> tuple2Dataset = cleanDs.groupByKey(acteDecesToKeyFunction, Encoders.STRING()).mapValues(getStatOnFunction, Encoders.bean(Stat.class)).reduceGroups(reduceFunction);
        tuple2Dataset.show();
        tuple2Dataset.printSchema();
        //cleanDs.show(5, false);
        return tuple2Dataset;
    }
}
