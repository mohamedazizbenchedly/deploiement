package Application.prix.functions;

import Application.prix.beans.Prix;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import java.io.Serializable;
import java.util.function.Function;


public class PrixMapper implements Function<Dataset<Row>, Dataset<Prix>> , Serializable {
    private final RowToPrix  parser = new RowToPrix();
    private final MapFunction<Row, Prix> task = parser::apply;

    @Override
    public Dataset<Prix> apply(Dataset<Row> inputDS) {
        return inputDS.map(task , Encoders.bean(Prix.class)) ;
    }
}
