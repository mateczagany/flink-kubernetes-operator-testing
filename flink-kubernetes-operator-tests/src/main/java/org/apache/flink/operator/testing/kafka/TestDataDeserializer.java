package org.apache.flink.operator.testing.kafka;

import org.apache.flink.operator.testing.TestData;

import org.apache.kafka.common.serialization.Deserializer;

public class TestDataDeserializer implements Deserializer<TestData> {
    @Override
    public TestData deserialize(String topic, byte[] bytes) {
        return TestData.deserialize(bytes);
    }
}
