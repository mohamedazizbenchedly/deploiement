package org.example.deces.functions.parser.parser;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.function.Function;
import org.example.deces.functions.parser.beans.ActeDeces;


@Getter
public class RowToActeDecesFunc implements Function<Row,ActeDeces>, Serializable  {

    @Override
    public ActeDeces apply(Row row) {
        //lorsque en utilise un sheama on doit indique le row avec la metadonnes
         String line  = row.getString(0) ;
        //String line = row.getAs("value") ;

        String[] nomPrenoms = StringUtils.splitByWholeSeparatorPreserveAllTokens(line.substring(0, 80), "*", 2);

        String nom = nomPrenoms[0];

        String prenom = nomPrenoms[1].trim().replace("/","");;

        String sex = line.substring(80,81);

        String dateNaissanceP = line.substring(81, 89);

        String codeLieux = line.substring(89,94);

        String localiteNaissance = line.substring(94,124);

        String libellePays = line.substring(124,153);

        String dateDeces = line.substring(154,162);

        String codeLieuxDeces = line.substring(162,167);

        String numeroActeDeces = line.substring(167,170);

        //sexe(Integer.parseInt(line.substring(80,81)))

        return ActeDeces.builder()
                .nom(nom)
                .prenoms(prenom)
                .sexe(sex)
                .dateNaissance(dateNaissanceP)
                .CodeLieuNaissance(codeLieux)
                .localiteNaissance(localiteNaissance)
                .libellePays(libellePays)
                .dateDeces(dateDeces)
                .codeLieuxDeces(codeLieuxDeces)
                .NumeroActeDeces(numeroActeDeces)
                .build();
    }
}
