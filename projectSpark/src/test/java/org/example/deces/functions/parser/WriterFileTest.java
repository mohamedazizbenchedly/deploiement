package org.example.deces.functions.parser;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.deces.functions.parser.writer.TextFileWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.example.deces.functions.parser.beans.ActeDeces;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.*;

public class WriterFileTest {
    private SparkSession sparkSession;
    private String outputPath = "src/test/resources/output";
    private TextFileWriter textFileWriter;

    @Before
    public void setUp() {
        sparkSession = SparkSession.builder()
                .appName("TextFileWriterTest")
                .master("local[*]")
                .getOrCreate();
        textFileWriter = new TextFileWriter(outputPath);
    }

    @Test
    public void testwriterFile() {
        // prepare test data
        List<ActeDeces> acteDecesList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ActeDeces acteDeces = new ActeDeces();
            acteDeces.setNom("Nom " + i);
            acteDecesList.add(acteDeces);
        }


        Dataset<Row> df = sparkSession.createDataFrame(acteDecesList, ActeDeces.class);
        Dataset<ActeDeces> acteDecesDataset = df.as(Encoders.bean(ActeDeces.class));

      //  Dataset<ActeDeces> acteDecesDataset = sparkSession.createDataFrame(acteDecesList, ActeDeces.class);

        textFileWriter.accept(acteDecesDataset);

        assertTrue(acteDecesDataset.count() == 10);

    }

    @After
    public void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}