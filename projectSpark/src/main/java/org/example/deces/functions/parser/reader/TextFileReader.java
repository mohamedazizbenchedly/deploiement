package org.example.deces.functions.parser.reader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@Slf4j

@RequiredArgsConstructor
public class TextFileReader implements Supplier<Dataset<Row>> {


    private final SparkSession sparkSession;
    private final String inputPath;

    @Override
    public Dataset<Row> get() {


        Dataset<Row> inputDS = sparkSession.read().text(inputPath);
        inputDS.printSchema();
        inputDS.show(10, false);

        return inputDS;

    }
}
