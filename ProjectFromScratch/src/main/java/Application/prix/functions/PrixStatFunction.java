package Application.prix.functions;

import Application.prix.beans.Prix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

public class PrixStatFunction  implements Function<Dataset<Row>, Dataset<Prix> >{
    @Override
    public Dataset<Prix> apply(Dataset<Row> rowDataset) {
        Dataset<Prix> cleanDs = new PrixMapper().apply(rowDataset) ;
        cleanDs.printSchema();
        cleanDs.show(5, false);
        return cleanDs;

    }
}
