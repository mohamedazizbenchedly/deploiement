package Application.prix.writer;

import Application.prix.beans.Prix;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Consumer;

@RequiredArgsConstructor
public class CsvWriter implements Consumer<Dataset<Prix>> {

    private  final String outputPath ;

    @Override
    public void accept(Dataset<Prix> prixDataset) {

        prixDataset.write().csv(outputPath);
    }


}
