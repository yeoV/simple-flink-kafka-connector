package examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaFlinkExample {

    final static String inputTopic = "from-topic";
    final static String outputTopic = "to-topic";
    final static String jobTitle = "Kafka Example";

    public static void main(String[] args) throws Exception {
//        final String bootStrapServers = "172.18.0.4:9092";
//        final String bootStrapServers = "192.168.3.199:9092";
        final String bootStrapServers = "localhost:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootStrapServers)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setKeySerializationSchema(new SimpleStringSchema())
                .setTopic(outputTopic)
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootStrapServers)
                .setRecordSerializer(serializer)
                .build();

        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Source Kafka "from-topic"
        System.out.println(text);

        text.sinkTo(sink);
        env.execute(jobTitle);
    }
}
