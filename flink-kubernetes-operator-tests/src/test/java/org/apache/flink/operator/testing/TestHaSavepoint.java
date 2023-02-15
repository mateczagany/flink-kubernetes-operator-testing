package org.apache.flink.operator.testing;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.operator.testing.app.FlinkAppArgumentsBuilder;
import org.apache.flink.operator.testing.app.FlinkAppManager;
import org.apache.flink.operator.testing.app.FlinkDeploymentBuilder;
import org.apache.flink.operator.testing.kafka.KafkaClient;
import org.apache.flink.operator.testing.properties.KafkaProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.operator.testing.Constants.TEST_JOB_ENTRYPOINT_HA;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_JAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;


@Slf4j
public class TestHaSavepoint extends TestBase {
    private static final String APP_NAME = "test-app";
    private static final Integer TEST_DATA_COUNT = 10000;
    private static final String KAFKA_TOPIC_SOURCE = "sourceTopic";
    private static final String KAFKA_TOPIC_SINK = "sinkTopic";
    private final List<TestData> expectedResult = new ArrayList<>();

    @AfterEach
    public void after() {
        flinkAppManager.deleteAppIfPresent(APP_NAME);
        kafkaClient.deleteTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));
    }

    public static Stream<Arguments> testHaLastState() {
        var deploymentModes = List.of(
                KubernetesDeploymentMode.NATIVE,
                KubernetesDeploymentMode.STANDALONE
        );
        var stateBackends = List.of(
                "memory",
                "rocks",
                "hashmap"
        );
        return Lists.cartesianProduct(deploymentModes, stateBackends).stream()
                .map(p -> Arguments.of(p.get(0), p.get(1)));
    }

    @ParameterizedTest
    @MethodSource
    public void testHaLastState(KubernetesDeploymentMode deploymentMode, String stateBackend) {
        flinkAppManager.deleteAppIfPresent(APP_NAME);

        kafkaClient.deleteTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));
        kafkaClient.createTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));

        produceRandomData();
        flinkAppManager.createFlinkDeployment(createFlinkDeployment(deploymentMode, stateBackend));
        waitAtMost(Duration.ofMinutes(3)).untilAsserted(this::assertSinkResult);
        flinkAppManager.stopApp(APP_NAME);
        produceRandomData();
        flinkAppManager.restartApp(APP_NAME);
        waitAtMost(Duration.ofMinutes(3)).untilAsserted(this::assertSinkResult);
    }

    private FlinkDeployment createFlinkDeployment(KubernetesDeploymentMode deploymentMode, String stateBackend) {
        var flinkConfiguration = Map.of(
                "state.savepoints.dir", "file:///flink-data/savepoints",
                "state.checkpoints.dir", "file:///flink-data/checkpoints",
                "high-availability", "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory",
                "high-availability.storageDir", "file:///flink-data/ha",
                "kubernetes.taskmanager.cpu.limit-factor", "1.3",
                "kubernetes.jobmanager.cpu.limit-factor", "1.3",
                "kubernetes.jobmanager.memory.limit-factor", "1.1",
                "kubernetes.taskmanager.memory.limit-factor", "1.1"
        );

        var args = new FlinkAppArgumentsBuilder()
                .addProperties(KafkaProperties.writeKafkaProperties(getFlinkKafkaProperties()))
                .addProperty("kafka.source.topic", KAFKA_TOPIC_SOURCE)
                .addProperty("kafka.sink.topic", KAFKA_TOPIC_SINK)
                .addProperty("kafka.properties.auto.offset.reset", "earliest")
                .addProperty("flink.state.backend", stateBackend)
                .build();

        var jobSpec = JobSpec.builder()
                .state(JobState.RUNNING)
                .jarURI(TEST_JOB_JAR)
                .entryClass(TEST_JOB_ENTRYPOINT_HA)
                .args(args)
                .upgradeMode(UpgradeMode.SAVEPOINT)
                .parallelism(2)
                .build();

        return new FlinkDeploymentBuilder()
                .withAppName(APP_NAME)
                .withFlinkImage("czmate10/flink-test-app:latest")
                .withJobSpec(jobSpec)
                .withKubernetesDeploymentMode(deploymentMode)
                .withJobManagerReplicas(3)
                .withTaskManagerReplicas(2)
                .withFlinkConfiguration(flinkConfiguration)
                .build();
    }

    private void produceRandomData() {
        var testRecords = IntStream.range(expectedResult.size(), expectedResult.size() + TEST_DATA_COUNT)
                .mapToObj(TestData::generateRandom)
                .collect(Collectors.toList());
        expectedResult.addAll(testRecords);

        try (var kafkaRecordWriter = kafkaClient.writer(KAFKA_TOPIC_SOURCE)) {
            kafkaRecordWriter.writeRecords(testRecords);
        }
    }

    private void assertSinkResult() {
        try (var kafkaRecordReader = kafkaClient.reader(KAFKA_TOPIC_SINK)) {
            var records = kafkaRecordReader.readRecords(expectedResult.size());
            log.info("Found {} records", records.size());
            assertThat(records).hasSameElementsAs(expectedResult);
        }
    }
}
