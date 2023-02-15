package examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadTopic {

    final static String bootStrapServers = "localhost:9092";
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootStrapServers)
                .setTopics("from-topic")
                .setGroupId("my-group")

                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        stream.print();
        env.execute("Topic Read Test");

//        KafkaSink<String> sink = KafkaSink.<String>builder()
//                .setBootstrapServers(bootStrapServers)
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic("to-topic").setValueSerializationSchema((new SimpleStringSchema())).build())
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .build();
//
//        stream.sinkTo(sink);
    }
}
