/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package examples;

import examples.model.AddressTopic;
import examples.model.GetPersonNameTopic;
import examples.schema.AddressTopicDeserializationSchema;
import examples.schema.GetPersonNameSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.util.Properties;

public class ReadTopic {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty("BOOTSTRAP_SERVERS", "localhost:9092");
        prop.setProperty("SOURCE_TOPIC", "address-topic");
        prop.setProperty("TARGET_TOPIC", "target-topic");
        kafkaToKafkaWithFlink(prop);
    }
    public static void kafkaToKafkaWithFlink(Properties prop) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /******************************************************************
         * Source Topic
         * ****************************************************************/
        // Flink Consume Kafka AddressTopic
        KafkaSource<AddressTopic> source = KafkaSource.<AddressTopic>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(prop.getProperty("SOURCE_TOPIC"))
                .setGroupId("my-group")
                .setValueOnlyDeserializer(new AddressTopicDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<AddressTopic> sources = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        sources.print();

        /******************************************************************
         * Target Topic
         * ****************************************************************/

        // Extract "Owner name"
        DataStream<GetPersonNameTopic> target = sources.flatMap((FlatMapFunction<AddressTopic, GetPersonNameTopic>) (in, out) -> out.collect(
                        new GetPersonNameTopic(in.getOwner())
                )).returns(GetPersonNameTopic.class)
                .keyBy(GetPersonNameTopic::getPersonName);
        target.print();


        KafkaRecordSerializationSchema serializer = KafkaRecordSerializationSchema.<GetPersonNameTopic>builder()
                .setTopic(prop.getProperty("TARGET_TOPIC"))
                .setValueSerializationSchema(new GetPersonNameSerializationSchema())
                .setPartitioner(new FlinkFixedPartitioner())
                .build();


        KafkaSink sink = KafkaSink.<String>builder()
                .setBootstrapServers(prop.getProperty("BOOTSTRAP_SERVERS"))
                .setRecordSerializer(serializer)
                .build();

        target.sinkTo(sink);
        env.execute("Topic Read Write Test");


    }
}
