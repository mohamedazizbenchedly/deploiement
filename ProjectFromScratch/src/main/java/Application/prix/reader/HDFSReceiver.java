package Application.prix.reader;

import Application.prix.beans.Prix;
import Application.prix.functions.TextToPrix;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class HDFSReceiver implements Supplier<JavaDStream<Prix>> {
    private final JavaStreamingContext javaStreamingContext;
    private final String inputPathStr;

    private final TextToPrix textToPrix = new TextToPrix();
    private final Function<String, Prix> mapper = textToPrix::apply;
    private final Function<Path, Boolean> filter = p -> p.getName().endsWith(".csv");

    @Override
    public JavaDStream<Prix> get() {
        JavaPairInputDStream<PrixLongWritable, PrixText> inputDStream = javaStreamingContext
                .fileStream(
                        inputPathStr,
                        PrixLongWritable.class,
                        PrixText.class,
                        PrixTextInputFormat.class,
                        filter,
                        true
                );
        inputDStream.print();
        return inputDStream.map(t -> t._2().toString()).map(mapper);


    }
}
