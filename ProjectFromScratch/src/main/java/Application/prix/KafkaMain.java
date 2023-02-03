package Application.prix;

import Application.prix.beans.Prix;
import Application.prix.reader.KafkaReceiver;
import Application.prix.writer.CsvWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.List;


@Slf4j
public class KafkaMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        log.info("Using kafka........");

        Config config = ConfigFactory.load("app.properties");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        String inputPathStr = config.getString("app.path.input");
        String outPathStr = config.getString("app.path.output");
        String checkpointStr = config.getString("app.path.checkpoint");

        List<String> topics = config.getStringList("app.kafka.topics");



        SparkSession sparkSession = SparkSession
                .builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate();

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        log.info("fileSystem got from sparkSession in the main : hdfs.getScheme = {}", hdfs.getScheme());

         final Function<String, Prix> mapper = null;
//

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(
                checkpointStr,
                () -> {
                    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),
                            new Duration(1000 * 10)
                    );
                    javaStreamingContext.checkpoint(checkpointStr);
//
                    KafkaReceiver kafkaReceiver = new KafkaReceiver(topics,javaStreamingContext);

                    JavaDStream<Prix> prixJavaDStream = kafkaReceiver.get();

                    prixJavaDStream.foreachRDD(
                            prixJavaRDD -> {
                                log.info("batch at {}", System.currentTimeMillis());
                                Dataset<Prix> prixDataset = SparkSession.active().createDataset(
                                        prixJavaRDD.rdd(),
                                        Encoders.bean(Prix.class)
                                ).cache();

                                prixDataset.printSchema();
                                prixDataset.show(10, false);
                                log.info("le nbre est {}", prixDataset.count());

                                CsvWriter writer = new CsvWriter(outPathStr);
                                writer.accept(prixDataset);
                                prixDataset.unpersist();
                                log.info("done..........");

//
                            }
                    );


                    return javaStreamingContext;
                },
                sparkSession.sparkContext().hadoopConfiguration()

        );
//
        jsc.start();
        jsc.awaitTermination();

    }
}
