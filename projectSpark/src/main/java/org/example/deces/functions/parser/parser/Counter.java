package org.example.deces.functions.parser.parser;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.example.deces.functions.parser.beans.ActeDeces;

import java.util.function.Function;


@Data
@RequiredArgsConstructor
public class Counter implements Function<Dataset<ActeDeces>, Long> {

    public Long apply(Dataset<ActeDeces> rowDataset) {

        return rowDataset.count() ;

    }
}
