package org.apache.flink.operator.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.io.IOException;
import java.util.Map;

@Data
@Builder
@Jacksonized
public class TestConfig {
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());
    private final Boolean createKubernetesCluster;
    private final Map<String, String> kafkaProperties;

    public static TestConfig load() {
        try (var in = TestConfig.class.getResourceAsStream("/config.yaml")) {
            return OBJECT_MAPPER.readValue(in, TestConfig.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

