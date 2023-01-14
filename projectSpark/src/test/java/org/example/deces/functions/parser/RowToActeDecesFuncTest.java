package org.example.deces.functions.parser;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.example.deces.functions.parser.beans.ActeDeces;
import org.example.deces.functions.parser.parser.RowToActeDecesFunc;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RowToActeDecesFuncTest {

    @Test
    public void testRowToActeDecesFunc() {

        RowToActeDecesFunc f = new RowToActeDecesFunc();

        Row input = RowFactory.create("KARAKUS*CEMAL/                                                                  11956112099208USAK                          TURQUIE                       2022111901004240                            ");

        ActeDeces  expected = ActeDeces.builder()
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
                .build()  ;

        ActeDeces actual = f.apply(input);
        assertThat(actual).isEqualTo(expected);


    }
}
