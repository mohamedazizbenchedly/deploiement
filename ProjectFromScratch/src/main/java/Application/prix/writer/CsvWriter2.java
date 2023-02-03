package Application.prix.writer;

import Application.prix.beans.Prix;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Consumer;

@RequiredArgsConstructor
public class CsvWriter2 implements Consumer<Dataset<Row>> {

    private  final String outputPath ;

    @Override
    public void accept(Dataset<Row> prixDataset) {
    }
}
