package org.apache.flink.operator.testing.kafka;

import org.apache.flink.operator.testing.TestData;

import org.apache.kafka.common.serialization.Serializer;

public class TestDataSerializer implements Serializer<TestData> {
    @Override
    public byte[] serialize(String topic, TestData testData) {
        return testData.serialize();
    }
}
