package test.unitaires;

import Application.prix.KafkaMain;
import Application.prix.reader.HdfsTextFileReader;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class KafkaMainTest {

   // Config config = ConfigFactory.load("app.properties");
    String outputPathStr = ConfigFactory.load("app.properties").getString("app.path.output");

    @Test
    public void test() throws IOException, InterruptedException {
        FileSystem localFs = FileSystem.getLocal(new Configuration());
        log.info("fileSystem used in the test : localFs.getScheme = {}", localFs.getScheme());

        KafkaMain.main(new String[0]);

        Path outputPath = new Path(outputPathStr);
        Stream<Path> csvFilePaths = Arrays.stream(localFs.listStatus(outputPath))
                .map(FileStatus::getPath)
                .filter(p -> p.getName().startsWith("part-") && p.toString().endsWith(".csv"));


        List<Stream<String>> collect = csvFilePaths
                .map(outputJsonFilePath -> new HdfsTextFileReader(localFs, (Path) csvFilePaths).get())
                .collect(Collectors.toList());



        assertThat(collect)
                .isNotEmpty();

    }

}
