package test.unitaires;

import Application.prix.beans.Prix;
import Application.prix.reader.PrixReader;
import Application.prix.writer.CsvWriter2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HadoopMainTestv2 {
    private SparkSession sparkSession;
    private Dataset<Row> prixDataset;

    @Before
    public void setUp() {
        sparkSession = SparkSession.builder().master("local").appName("test").getOrCreate();

        List<Prix> prixList = Arrays.asList(
                new Prix("id1", "1", "product1", "1.0"),
                new Prix("id2", "2", "product2", "2.0"),
                new Prix("id3", "3", "product3", "3.0")
        );

        prixDataset = sparkSession.createDataFrame(prixList, Prix.class);
    }

    @After
    public void tearDown() {
        sparkSession.stop();
    }

    @Test
    public void test() throws InterruptedException, IOException {
        String outputPathStr = "target/test-classes/data/outputckpp";

        CsvWriter2 writer2 = new CsvWriter2(outputPathStr);
        writer2.accept(prixDataset);

        String inputPathStr = "src/test/resources/data/input/Niveaux_prix_TRVG.csv";
        PrixReader reader = PrixReader.builder()
                .sparkSession(sparkSession)
                .inputPathStr(inputPathStr)
                .build();

        Dataset<Prix> readPrixDataset = reader.get();

        assertEquals(prixDataset.count(), readPrixDataset.count());

    }
}
