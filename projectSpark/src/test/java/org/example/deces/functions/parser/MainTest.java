package org.example.deces.functions.parser;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

import org.example.MainProg;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


@Slf4j
public class MainTest {
    String outPathStr = ConfigFactory.load().getString("app.path.output") ;

    @Test
    public void testMain() throws IOException {


        MainProg.main(new String[0]);
        Path outputpath = Paths.get(outPathStr);
        Optional<Path> outputPathFileOp = Files.list(outputpath)
                .filter(p -> p.getFileName().toString().startsWith("part-0000") && p.getFileName().toString().endsWith(".csv"))
                .findFirst();

        List<String> liens = outputPathFileOp
                .map(outputFilePath -> {
                    List<String> csvfileContents = Collections.emptyList() ;
                            try {
                                csvfileContents = Files.readAllLines(outputFilePath) ;
                                } catch (IOException e) {
                                  log.info("could not read outputFilePath ...", outputFilePath);
                                }
                    return csvfileContents ;
                })
                .orElse(Collections.emptyList()) ;

        assertThat(liens)
                .isNotEmpty() ;
//                .hasSize(56669) ;


    }
}
