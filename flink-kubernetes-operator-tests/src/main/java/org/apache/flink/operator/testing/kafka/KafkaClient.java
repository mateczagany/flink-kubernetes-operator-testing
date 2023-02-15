package org.apache.flink.operator.testing.kafka;

import org.apache.flink.operator.testing.TestData;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class KafkaClient {
    private final Map<String, String> kafkaProperties;

    public KafkaClient(Map<String, String> kafkaProperties) {
        this.kafkaProperties = new HashMap<>(kafkaProperties);
    }

    public void createTopics(List<String> topicNames) {
        try (var adminClient = Admin.create(new HashMap<>(kafkaProperties))) {
            adminClient.createTopics(
                    topicNames.stream()
                            .map(t -> new NewTopic(t, 10, (short) 0))
                            .collect(Collectors.toList()));
        }
    }

    public void deleteTopics(List<String> topicNames) {
        try (var adminClient = Admin.create(new HashMap<>(kafkaProperties))) {
            adminClient.deleteTopics(topicNames);
        }
    }

    public KafkaRecordWriter<String, TestData> writer(String topicName) {
        var producerConfig = new HashMap<String, Object>(kafkaProperties);
        producerConfig.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TestDataSerializer.class.getName());

        return new KafkaRecordWriter<>(producerConfig, topicName);
    }

    public KafkaRecordReader<String, TestData> reader(String topicName) {
        var consumerProperties = new HashMap<String, Object>(kafkaProperties);
        consumerProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                TestDataDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new KafkaRecordReader<>(consumerProperties, topicName);
    }
}
