package org.apache.flink.operator.testing.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Map;

public class KafkaRecordWriter<K, V> implements AutoCloseable {
    private final KafkaProducer<K, V> kafkaProducer;
    private final String topicName;

    KafkaRecordWriter(Map<String, Object> kafkaProperties, String topicName) {
        kafkaProducer = new KafkaProducer<>(kafkaProperties);
        this.topicName = topicName;
    }

    public void writeRecords(List<V> records) {
        records.forEach(record -> kafkaProducer.send(new ProducerRecord<>(topicName, record)));
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}
