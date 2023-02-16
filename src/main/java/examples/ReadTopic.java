package examples;

import examples.model.FirstTopic;
import examples.model.Target;
import examples.schema.FirstTopicDeserializationSchema;
import examples.schema.TargetSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ReadTopic {
    public static void main(String[] args) throws Exception {
        Properties prop = getProperties();
        kafkaToKafkaWithFlink(prop);
    }

    /**
     * resources/config.properties load
     *
     * @return Properties
     */
    private static Properties getProperties() {
        Properties prop = new Properties();

        try (InputStream propsInput =
                     ReadTopic.class.getClassLoader().getResourceAsStream("config.properties")) {
            prop.load(propsInput);
            return prop;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    public static void kafkaToKafkaWithFlink(Properties prop) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Flink Source
        KafkaSource<FirstTopic> source = KafkaSource.<FirstTopic>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(prop.getProperty("FIRST_TOPIC"))
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new FirstTopicDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<FirstTopic> sources = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        sources.print();
        DataStream<Target> target = sources.flatMap((FlatMapFunction<FirstTopic, Target>) (in, out) -> out.collect(
                        new Target(in.getTable())
                )).returns(Target.class)
                .keyBy(Target::getName);
        target.print();

        env.execute("Topic Read Write Test");

        KafkaRecordSerializationSchema serializer = KafkaRecordSerializationSchema.<Target>builder()
                .setTopic(prop.getProperty("TARGET_TOPIC"))
                .setValueSerializationSchema(new TargetSerializationSchema())
                .setPartitioner(new FlinkFixedPartitioner())
                .build();


        KafkaSink sink = KafkaSink.<String>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setRecordSerializer(serializer)
                .build();

        target.sinkTo(sink);
//        env.execute("Topic Read Write Test");


    }
}
