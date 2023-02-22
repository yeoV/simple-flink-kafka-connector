package examples.schema;

import examples.model.AddressTopic;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class AddressTopicDeserializationSchema implements DeserializationSchema<AddressTopic> {

    @Override
    public AddressTopic deserialize(byte[] bytes) throws IOException {
        // deserialize JSON object
        return new ObjectMapper().readValue(bytes, AddressTopic.class);
    }

    @Override
    public boolean isEndOfStream(AddressTopic addressTopic) {
        return false;
    }

    @Override
    public TypeInformation<AddressTopic> getProducedType() {
        return TypeInformation.of(AddressTopic.class);
    }

}
