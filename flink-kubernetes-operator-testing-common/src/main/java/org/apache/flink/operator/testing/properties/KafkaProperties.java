package org.apache.flink.operator.testing.properties;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@UtilityClass
@Slf4j
public class KafkaProperties {
    public static final String KAFKA_PREFIX = "kafka.properties.";

    public static Map<String, String> readKafkaProperties(Map<String, String> properties) {
        var kafkaProperties = new HashMap<String, String>();
        for (var entry : properties.entrySet()) {
            if (entry.getKey().startsWith(KAFKA_PREFIX)) {
                var key = entry.getKey().substring(KAFKA_PREFIX.length());
                log.info("Kafka property :: {} = {}", key, entry.getValue());
                kafkaProperties.put(key, entry.getValue());
            }
        }
        return kafkaProperties;
    }

    public static Map<String, String> writeKafkaProperties(Map<String, String> kafkaProperties) {
        var properties = new HashMap<String, String>();
        for (var entry : kafkaProperties.entrySet()) {
            properties.put(KAFKA_PREFIX + entry.getKey(), entry.getValue());
        }
        return properties;
    }
}
