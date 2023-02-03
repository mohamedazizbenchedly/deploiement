package Application.prix.functions;


import Application.prix.beans.Prix;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;

import java.io.Serializable;

@Slf4j
public class RowToPrixSparkFunc implements MapFunction<String, Prix> , Serializable  {

    @Override
    public Prix call(String line) {
        log.info("current line = {}", line);

        String[] fields = line.split(";");
        //       Row row = RowFactory.create(fields);

        String CODE_INSEE = fields[0];
        String CODE_POSTAL = fields[1];
        String LIBELLE_COMMUNE =fields[2];
        String NIVEAU_PRIX = fields[3];

        return Prix.builder()
                .codeInsee(CODE_INSEE)
                .codePostal(CODE_POSTAL)
                .libelleCommune(LIBELLE_COMMUNE)
                .niveauxPrix(NIVEAU_PRIX)
                .build();
    }
}
