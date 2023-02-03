package Application.prix.reader;

import Application.prix.beans.Prix;
import Application.prix.functions.TextToPrix;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class KafkaReceiver implements Supplier<JavaDStream<Prix>> {

    private final List<String> topics;
    private final JavaStreamingContext javaStreamingContext;


    private final Map<String, Object> kafkaParams = new HashMap<String, Object>() {{
        put("bootstrap.servers", "localhost:9092");
        put("key.deserializer", StringDeserializer.class);
        put("value.deserializer", StringDeserializer.class);
        put("group.id", "spark-kafka-integ");
        put("auto.offset.rest", "earliest");

    }};

    @Override
    public JavaDStream<Prix> get() {

        JavaInputDStream<ConsumerRecord<String, String>> directDStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        TextToPrix stringToPrix = new TextToPrix() ;

        JavaDStream<Prix> javaDStream = directDStream.map(ConsumerRecord::value).map(stringToPrix::apply);;
        return javaDStream;

    }
}


