package org.apache.flink.operator.testing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

@Data
@Builder
@Jacksonized
public class TestData {
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private final Integer id;
    private final String name;
    private final Integer quantity;
    private final Float price;
    private final Long sleepMillis;

    public static TestData generateRandom(Integer id) {
        return generateRandom(id, 0L);
    }

    public static TestData generateRandom(Integer id, Long sleepMillis) {
        return new TestData(
                id,
                UUID.randomUUID().toString(),
                RANDOM.nextInt(100),
                RANDOM.nextInt(1000000) / 100f,
                sleepMillis);
    }

    public static TestData deserialize(byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, TestData.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] serialize() {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
