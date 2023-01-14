package org.example.deces.functions.parser.beans;

import lombok.*;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class ActeDeces implements Serializable {

   private String nom;
   private String prenoms;
   private String sexe ;
   private String  dateNaissance ;
   private String CodeLieuNaissance ;
   private String localiteNaissance ;
   private String libellePays ;
   private String dateDeces ;
   private String codeLieuxDeces ;
   private String NumeroActeDeces ;

}
