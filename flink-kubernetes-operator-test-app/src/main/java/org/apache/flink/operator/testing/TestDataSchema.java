package org.apache.flink.operator.testing;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class TestDataSchema
        implements DeserializationSchema<TestData>, SerializationSchema<TestData> {

    @Override
    public TestData deserialize(byte[] bytes) {
        return TestData.deserialize(bytes);
    }

    @Override
    public boolean isEndOfStream(TestData s) {
        return false;
    }

    @Override
    public byte[] serialize(TestData testData) {
        return testData.serialize();
    }

    @Override
    public TypeInformation<TestData> getProducedType() {
        return new TypeHint<TestData>() {}.getTypeInfo();
    }
}
