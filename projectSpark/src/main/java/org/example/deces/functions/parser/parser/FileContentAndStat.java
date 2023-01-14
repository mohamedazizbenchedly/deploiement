package org.example.deces.functions.parser.parser;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.deces.functions.parser.beans.ActeDeces;
import org.example.deces.functions.parser.parser.ActeDecesMapper;

import java.util.function.Function;


@Data
@RequiredArgsConstructor
public class FileContentAndStat implements Function<Dataset<Row> , Dataset<ActeDeces>> {

    public Dataset<ActeDeces> apply(Dataset<Row> rowDataset) {

        Dataset<ActeDeces> cleanDs = new ActeDecesMapper().apply(rowDataset);
        cleanDs.printSchema();
        cleanDs.show(5, false);

        return cleanDs ;

    }
}
