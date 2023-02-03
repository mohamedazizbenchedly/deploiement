package Application.prix.beans;

import java.io.Serializable;

import lombok.*;

@AllArgsConstructor
@RequiredArgsConstructor
@Getter
@Builder
@Data

public class Prix implements Serializable {

    private String codeInsee;
    private String codePostal;
    private String libelleCommune;
    private String niveauxPrix;

}