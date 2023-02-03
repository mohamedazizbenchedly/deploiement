package Application.prix.functions;

import Application.prix.beans.Prix;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.function.Function;

public class RowToPrix implements Function<Row, Prix> , Serializable {

    @Override
    public Prix apply(Row row) {
        String CODE_INSEE = row.getAs("CODE_INSEE") ;
        String CODE_POSTAL = row.getAs("CODE_POSTAL") ;
        String LIBELLE_COMMUNE = row.getAs("LIBELLE_COMMUNE") ;
        String NIVEAU_PRIX = row.getAs("NIVEAU_PRIX") ;



        return Prix.builder()
                .codeInsee(CODE_INSEE)
                .codePostal(CODE_POSTAL)
                .libelleCommune(LIBELLE_COMMUNE)
                .niveauxPrix(NIVEAU_PRIX)
                .build();
    }
}
