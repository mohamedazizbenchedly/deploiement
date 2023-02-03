package Application.prix;

import Application.prix.functions.PrixStatFunction;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkApp {

    public static void main(String[] args) {

        Config config = ConfigFactory.load("app.properties") ;
        String masterUrl = config.getString("master") ;
        String appName = config.getString("appName") ;
        SparkSession spark = SparkSession.builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate() ;

        String inputPath = config.getString("app.data.input") ;
        String outputPath = config.getString("app.data.output") ;

        PrixStatFunction prixStatFunction = new PrixStatFunction() ;

    //    CsvReader csvReader = new CsvReader(spark ,inputPath,h) ;
    //    CsvWriter csvWriter = new CsvWriter(outputPath) ;

    //    csvWriter.accept(prixStatFunction.apply(csvReader.get()));


    }
}
