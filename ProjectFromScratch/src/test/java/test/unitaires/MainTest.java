package test.unitaires;
import Application.prix.hadoopMain;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class MainTest {
    //ConfigFactory.load()
    String outPathStr = ConfigFactory.load("app.properties").getString("app.path.output") ;

    @Test
    public void testMain() throws IOException, InterruptedException {
        FileSystem localFs = FileSystem.getLocal(new Configuration());
        log.info("fileSystem used in the test : localFs.getScheme = {}", localFs.getScheme());


        hadoopMain.main(new String[0]);
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


   }
}

