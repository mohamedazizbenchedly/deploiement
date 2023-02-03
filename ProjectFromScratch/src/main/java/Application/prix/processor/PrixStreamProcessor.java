package Application.prix.processor;

import Application.prix.beans.Prix;
import Application.prix.writer.CsvWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

@Slf4j
@RequiredArgsConstructor
public class PrixStreamProcessor implements VoidFunction<JavaRDD<Prix>> {
    private final String outputPathStr;


    @Override
    public void call(JavaRDD<Prix> prixJavaRDD) throws Exception {
        long ts = System.currentTimeMillis();
        log.info("micro-batch stored in folder={}", ts);

        if (prixJavaRDD.isEmpty()) {
            log.info("no data found!");
            return;
        }

        log.info("data under processing...");
        final SparkSession sparkSession = SparkSession.active();


        Dataset<Prix> prixDataset = sparkSession.createDataset(
                prixJavaRDD.rdd(),
                Encoders.bean(Prix.class)
        );


        prixDataset.printSchema();
        prixDataset.show(5, false);

        log.info("nb actesDeces = {}", prixDataset.count());


        CsvWriter writer = new CsvWriter(outputPathStr + "/time=" + ts);
        writer.accept(prixDataset);

        prixDataset.unpersist();
        log.info("done");
    }
}