package org.example.deces.functions.parser.writer;
import org.apache.spark.sql.Dataset;


import org.apache.spark.sql.SaveMode;
import org.example.deces.functions.parser.beans.ActeDeces;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class TextFileWriter implements Consumer<Dataset<ActeDeces>> {

    private final String outputPath;
    @Override
    public void accept(Dataset<ActeDeces> acteDecesDataset) {

        acteDecesDataset.write().mode(SaveMode.Overwrite).csv(outputPath);

    }


    public void countnbre(Long p ) {

        log.info("the nombre of line in your Data   ==> "+p.toString());

    }

}
