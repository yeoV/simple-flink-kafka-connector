package examples.schema;

import examples.model.FirstTopic;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class FirstTopicDeserializationSchema implements DeserializationSchema<FirstTopic> {

    @Override
    public FirstTopic deserialize(byte[] bytes) throws IOException {
        // deserialize JSON object
        return new ObjectMapper().readValue(bytes, FirstTopic.class);
    }

    @Override
    public boolean isEndOfStream(FirstTopic firstTopic) {
        return false;
    }

    @Override
    public TypeInformation<FirstTopic> getProducedType() {
        return TypeInformation.of(FirstTopic.class);
    }

}
