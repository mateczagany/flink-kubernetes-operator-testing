package org.apache.flink.operator.testing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.operator.testing.properties.KafkaProperties;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class BaseJob {

    protected final ParameterTool parameters;
    protected final StreamExecutionEnvironment env;
    private final Properties kafkaProperties = new Properties();

    public BaseJob(ParameterTool parameters) {
        this.parameters = parameters;
        kafkaProperties.putAll(KafkaProperties.readKafkaProperties(parameters.toMap()));
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    protected DataStreamSource<TestData> source() {
        var kafkaSource =
                KafkaSource.<TestData>builder()
                        .setProperties(kafkaProperties)
                        .setTopics(parameters.get("kafka.source.topic"))
                        .setValueOnlyDeserializer(new TestDataSchema())
                        .build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Source");
    }

    protected KafkaSink<TestData> sink() {
        var builder =
                KafkaSink.<TestData>builder()
                        .setKafkaProducerConfig(kafkaProperties)
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<TestData>builder()
                                        .setTopic(parameters.get("kafka.sink.topic"))
                                        .setValueSerializationSchema(new TestDataSchema())
                                        .build());

        if (parameters.has("kafka.transactional.id.prefix")) {
            builder =
                    builder.setTransactionalIdPrefix(
                            parameters.get("kafka.transactional.id.prefix"));
        }

        return builder.build();
    }
}
