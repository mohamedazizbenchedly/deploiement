package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.deces.functions.parser.beans.ActeDeces;
import org.example.deces.functions.parser.parser.ActeDecesStatFunction;
import org.example.deces.functions.parser.parser.Counter;
import org.example.deces.functions.parser.parser.ActeDecesMapper;
import org.example.deces.functions.parser.parser.FileContentAndStat;
import org.example.deces.functions.parser.reader.TextFileReader;
import org.example.deces.functions.parser.writer.TextFileWriter;

import java.beans.Encoder;

@Slf4j
public class MainProg {
    public static void main(String[] args) {


        System.out.println("Fichier des personnes décédées ");

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.name");

        SparkSession sparkSession = SparkSession
                .builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate();

        String inputPathStr = config.getString("app.path.input");
        String outPathStr = config.getString("app.path.output");

        final TextFileReader reader = new TextFileReader(sparkSession,inputPathStr);

        final TextFileWriter writer = new TextFileWriter(outPathStr);

        final FileContentAndStat fileContentAndStat = new FileContentAndStat() ;

        writer.accept(fileContentAndStat.apply(reader.get()));

        Dataset<Row> inputDS = sparkSession.read().text(inputPathStr);
        inputDS.printSchema();

        Dataset<ActeDeces> cleanDS = new ActeDecesMapper().apply(inputDS);
        cleanDS.printSchema();
        cleanDS.show(10, false);

        //faire un groupe by
        Dataset<Row> cleanDS2 = cleanDS.groupBy("sexe").count().as("le nbre ");
        cleanDS2.show();


//        // groupe By
//        String keyToRegroup = "sexe" ;
//        ActeDecesStatFunction acteDecesStatFunction = new ActeDecesStatFunction(keyToRegroup) ;
//        acteDecesStatFunction.apply(reader.get()) ;





//        final Counter counter = new Counter() ;
//        writer.countnbre(counter.apply(reader.get()));
//




//

    }
}