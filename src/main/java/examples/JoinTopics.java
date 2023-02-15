package examples;

import examples.model.FirstTopic;
import examples.model.JoinTopic;
import examples.model.SecondTopic;
import examples.schema.FirstTopicDeserializationSchema;
import examples.schema.JoinTopicSerializationSchema;
import examples.schema.SecondTopicDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class JoinTopics {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty("BOOTSTRAP_SERVERS", "localhost:9092");
        prop.setProperty("FIRST_TOPIC", "first-topic");
        prop.setProperty("SECOND_TOPIC", "second-topic");
        prop.setProperty("JOIN_TOPIC", "join-topic");

        runJoinTopics(prop);
    }

    public static void runJoinTopics(Properties prop) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // First Topic Source
        KafkaSource<FirstTopic> firstSource = KafkaSource.<FirstTopic>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(prop.getProperty("FIRST_TOPIC"))
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new FirstTopicDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<FirstTopic> FirstStream = env.fromSource(
                firstSource, WatermarkStrategy.noWatermarks(),"FirstTopic");

        tableEnv.createTemporaryView("firstTopic",FirstStream);
        // Second Topic Source
        KafkaSource<SecondTopic> secondSource = KafkaSource.<SecondTopic>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(prop.getProperty("SECOND_TOPIC"))
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new SecondTopicDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<SecondTopic> SecondStream = env.fromSource(
                secondSource, WatermarkStrategy.noWatermarks(),"SecondTopic");

        tableEnv.createTemporaryView("secondTopic",SecondStream);

        Table result =
                tableEnv.sqlQuery(
                        "SELECT " +
                                "firstTopic.name, " +
                                "firstTopic.age, " +
                                "secondTopic.address " +
                                "FROM " +
                                "firstTopic " +
                                "JOIN secondTopic " +
                                "ON firstTopic.name = secondTopic.name"

                );

        DataStream<JoinTopic> joinStream = tableEnv.toDataStream(result,
                JoinTopic.class);


        KafkaSink sink = KafkaSink.<JoinTopic>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(prop.getProperty("JOIN_TOPIC"))
                                .setValueSerializationSchema(new JoinTopicSerializationSchema())
                                .build()
                        ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        joinStream.sinkTo(sink);

        env.execute("Join test");
    }
}
