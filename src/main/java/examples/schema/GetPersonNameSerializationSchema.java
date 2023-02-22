package examples.schema;

import examples.model.GetPersonNameTopic;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class GetPersonNameSerializationSchema implements SerializationSchema<GetPersonNameTopic> {

    @Override
    public byte[] serialize(GetPersonNameTopic getPersonNameTopic) {
        try {
            return new ObjectMapper().writeValueAsString(getPersonNameTopic).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }
}
