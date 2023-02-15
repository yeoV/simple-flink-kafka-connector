package examples.schema;

import examples.model.SecondTopic;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class SecondTopicDeserializationSchema implements DeserializationSchema<SecondTopic> {

    @Override
    public SecondTopic deserialize(byte[] bytes) throws IOException {
        // deserialize JSON object
        return new ObjectMapper().readValue(bytes, SecondTopic.class);
    }

    @Override
    public boolean isEndOfStream(SecondTopic secondTopic) {
        return false;
    }

    @Override
    public TypeInformation<SecondTopic> getProducedType() {
        return TypeInformation.of(SecondTopic.class);
    }

}
