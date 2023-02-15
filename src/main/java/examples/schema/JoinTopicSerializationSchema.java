package examples.schema;

import examples.model.JoinTopic;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JoinTopicSerializationSchema implements SerializationSchema<JoinTopic> {

    @Override
    public byte[] serialize(JoinTopic joinTopic) {
        try {
            return new ObjectMapper().writeValueAsString(joinTopic).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }
}
