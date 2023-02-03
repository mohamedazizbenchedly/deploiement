package Application.prix.reader;

import Application.prix.beans.Prix;
import Application.prix.functions.PrixMapper2;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.function.Supplier;

@Builder
//@RequiredArgsConstructor
//@NoArgsConstructor
public class PrixReader implements Supplier<Dataset<Prix>> {
    private SparkSession sparkSession;
    private FileSystem hdfs;
    private String inputPathStr;

    private final PrixMapper2 prixMapper = new PrixMapper2();


    @Override
    public Dataset<Prix> get() {
        try {
            if(hdfs.exists(new Path(inputPathStr))) {
                Dataset<String> rowDataset = sparkSession.read().textFile(inputPathStr);
                rowDataset.printSchema();
                rowDataset.show(5, false);


                return prixMapper.apply(rowDataset);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sparkSession.emptyDataset(Encoders.bean(Prix.class));
    }

}