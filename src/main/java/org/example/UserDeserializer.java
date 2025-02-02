package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class UserDeserializer implements Deserializer<User> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, User.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing User object", e);
        }
    }
}
