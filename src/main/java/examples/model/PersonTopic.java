package examples.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
@NoArgsConstructor
@AllArgsConstructor
@Data
public class PersonTopic {
    @JsonProperty("name")
    String name;

    @JsonProperty("age")
    String age;

    @JsonProperty("gender")
    String gender;

}
