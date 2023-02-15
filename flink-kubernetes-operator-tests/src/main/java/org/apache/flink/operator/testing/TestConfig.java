package org.apache.flink.operator.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.io.IOException;

@Data
@Builder
@Jacksonized
public class TestConfig {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private final String test;

    public static TestConfig load() {
        try (var in = TestConfig.class.getResourceAsStream("/config.yaml")) {
            return OBJECT_MAPPER.readValue(in, TestConfig.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
