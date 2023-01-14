package org.example.deces.functions.parser.parser;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.example.deces.functions.parser.beans.ActeDeces;


@AllArgsConstructor
@RequiredArgsConstructor
@Getter
public class ActeDecesToKeyFunction implements MapFunction <ActeDeces, String>{


    private String key ;


    @Override
    public String call(ActeDeces acteDeces) throws Exception {

        if (this.key.equals("sexe")) {
            return acteDeces.getSexe() ;
        }

        else if (this.key.equals("nom") ) {
            return acteDeces.getNom();
        }


        return "";
    }
}
