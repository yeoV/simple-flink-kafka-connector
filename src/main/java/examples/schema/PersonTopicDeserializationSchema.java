package examples.schema;

import examples.model.PersonTopic;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class PersonTopicDeserializationSchema implements DeserializationSchema<PersonTopic> {

    @Override
    public PersonTopic deserialize(byte[] bytes) throws IOException {
        // deserialize JSON object
        return new ObjectMapper().readValue(bytes, PersonTopic.class);
    }

    @Override
    public boolean isEndOfStream(PersonTopic personTopic) {
        return false;
    }

    @Override
    public TypeInformation<PersonTopic> getProducedType() {
        return TypeInformation.of(PersonTopic.class);
    }

}
