package org.example.deces.functions.parser;


import org.apache.spark.sql.*;
import org.example.deces.functions.parser.beans.ActeDeces;
import org.example.deces.functions.parser.parser.ActeDecesMapper;
import org.example.deces.functions.parser.parser.FileContentAndStat;
import org.junit.Test;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ContentAndStatTest {

    @Test
    public void testContentAndStatTest() {

        ActeDecesMapper f = new ActeDecesMapper() ;

        SparkSession sparksession = SparkSession
                .builder()
                .master("local[2]")
                .getOrCreate();

      //  Dataset<Row> input = sparksession.read().text("src/test/resources/data/input/deces-2022-m11.txt");
        Dataset<Row> input = (Dataset<Row>) RowFactory.create("KARAKUS*CEMAL/                                                                  11956112099208USAK                          TURQUIE                       2022111901004240                            ");


        Dataset<ActeDeces> expected = sparksession.createDataset(
                List.of(ActeDeces.builder()
                        .nom("KARAKUS")
                        .prenoms("CEMAL")
                        .sexe("1")
                        .dateNaissance("19561120")
                        .CodeLieuNaissance("99208")
                        .localiteNaissance("USAK                          ")
                        .libellePays("TURQUIE                      ")
                        .dateDeces("20221119")
                        .codeLieuxDeces("01004")
                        .NumeroActeDeces("240")
                        .build()) ,
                Encoders.bean(ActeDeces.class) );

        Dataset<ActeDeces> actual = f.apply(input);
        assertThat(actual).isEqualTo(expected);

    }
}
