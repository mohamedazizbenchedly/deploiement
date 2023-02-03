package test.unitaires;


import org.apache.spark.sql.Row;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.junit.Test;
import Application.prix.beans.Prix;
import Application.prix.functions.RowToPrix;

import static org.assertj.core.api.Assertions.assertThat;

public class RowToPrixTestUnitaire {

    @Test
    public void testRowToActeDecesFunc() {

        RowToPrix f = new RowToPrix();
        StructType sheama = new StructType(
                new StructField[]{
                        new StructField(
                                "CODE_INSEE",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "CODE_POSTAL",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "LIBELLE_COMMUNE",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),
                        new StructField(
                                "NIVEAU_PRIX",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                }
        );

        String values[] = {"01004", "01500", "AMBERIEU EN BUGEY", "3"} ;
        Row row = new GenericRowWithSchema(values , sheama);
        Prix expected = Prix.builder()
                .codePostal("01500")
                .libelleCommune("AMBERIEU EN BUGEY")
                .codeInsee("01004")
                .niveauxPrix("3")
                .build();

        Prix actual = f.apply(row);


        assertThat(actual).isEqualTo(expected);

    }
}

