package org.example.deces.functions.parser;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.deces.functions.parser.reader.TextFileReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

        import static org.junit.Assert.*;

public class ReaderFileTest {

    private SparkSession sparkSession;
    private String inputPath = "C:\\\\Users\\\\ben chedly\\\\Desktop\\\\exploitation de donnees\\\\projectSpark\\\\src\\\\main\\\\resources\\\\deces-2022-m11.txt";
    private TextFileReader textFileReader;

    @Before
    public void setUp() {
        sparkSession = SparkSession.builder()
                .appName("TextFileReaderTest")
                .master("local[*]")
                .getOrCreate();
        textFileReader = new TextFileReader(sparkSession, inputPath);
    }

    @Test
    public void testreaderFile() {
        Dataset<Row> inputDS = textFileReader.get();
        assertNotNull(inputDS);
        inputDS.printSchema();
        inputDS.show(10, false);
    }

    @After
    public void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }
}