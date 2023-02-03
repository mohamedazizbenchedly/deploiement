package Application.prix.functions;

import Application.prix.beans.Prix;

import java.io.Serializable;
import java.util.function.Function;

public class TextToPrix implements Function<String, Prix> , Serializable {

    @Override
    public Prix apply(String line ) {

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
