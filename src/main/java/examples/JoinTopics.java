package examples;

import examples.model.AddressTopic;
import examples.model.JoinTopic;
import examples.model.PersonTopic;
import examples.schema.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class JoinTopics {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty("BOOTSTRAP_SERVERS", "localhost:9092");
        prop.setProperty("FIRST_TOPIC", "address-topic");
        prop.setProperty("SECOND_TOPIC", "person-topic");
        prop.setProperty("JOIN_TOPIC", "join-topic");

        runJoinTopics(prop);
    }

    public static void runJoinTopics(Properties prop) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /******************************************************************
         * AddressInfo Topic
         * ****************************************************************/
        KafkaSource<AddressTopic> addressSource = KafkaSource.<AddressTopic>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(prop.getProperty("FIRST_TOPIC"))
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new AddressTopicDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<AddressTopic> addressStream = env.fromSource(
                addressSource, WatermarkStrategy.noWatermarks(),"AddressTopic");

        tableEnv.createTemporaryView("AddressTopic",addressStream);

        /******************************************************************
         * Person Topic
         * ****************************************************************/
        KafkaSource<PersonTopic> personSource = KafkaSource.<PersonTopic>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(prop.getProperty("SECOND_TOPIC"))
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new PersonTopicDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<PersonTopic> personStream = env.fromSource(
                personSource, WatermarkStrategy.noWatermarks(),"PersonTopic");

        tableEnv.createTemporaryView("PersonTopic",personStream);


        /******************************************************************
         * Person Topic
         * ****************************************************************/
        Table result =
                tableEnv.sqlQuery(
                        "SELECT " +
                                "AddressTopic.owner, " +
                                "AddressTopic.address, " +
                                "PersonTopic.age " +
                                "FROM " +
                                "AddressTopic " +
                                "JOIN PersonTopic " +
                                "ON AddressTopic.owner = PersonTopic.name"

                );



        DataStream<Row> resultRowStream = tableEnv.toChangelogStream(result);

        DataStream<String> joinedStringStream = resultRowStream.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                return row.toString() + setTimeStamp();
            }
        });


        KafkaSink sink = KafkaSink.<String>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(prop.getProperty("JOIN_TOPIC"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // print to /log/*-executor-*.log
        joinedStringStream.print();
        joinedStringStream.sinkTo(sink);


        env.execute("Flink Streaming Join Table Topic");

    }
    public static String setTimeStamp(){
        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss.SS z");
        Date date = new Date(System.currentTimeMillis());
        return formatter.format(date);
    }
}
