package org.apache.flink.operator.testing.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class KafkaRecordReader<K, V> implements AutoCloseable {
    private final KafkaConsumer<K, V> kafkaConsumer;

    KafkaRecordReader(Map<String, Object> kafkaProperties, String topicName) {
        kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        kafkaConsumer.subscribe(List.of(topicName));
    }

    public List<V> readRecords(Integer expectedSize) {
        var results = new ArrayList<V>();
        long end = System.currentTimeMillis() + 30 * 1000;

        while (System.currentTimeMillis() < end) {
            for (var record : kafkaConsumer.poll(Duration.ofSeconds(3))) {
                results.add(record.value());
                if (expectedSize != null && results.size() == expectedSize) {
                    return results;
                }
            }
        }

        return results;
    }

    public List<V> readRecords() {
        return readRecords(null);
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }
}
