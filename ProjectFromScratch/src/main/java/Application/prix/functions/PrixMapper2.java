package Application.prix.functions;

import Application.prix.beans.Prix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import java.util.function.Function;

public class PrixMapper2 implements Function<Dataset<String>, Dataset<Prix>> {

    private final RowToPrixSparkFunc task = new RowToPrixSparkFunc();


    @Override
    public Dataset<Prix> apply(Dataset<String> inputDS) {
        return inputDS.map(task, Encoders.bean(Prix.class));
    }
}

