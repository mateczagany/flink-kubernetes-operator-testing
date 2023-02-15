package org.apache.flink.operator.testing;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.operator.testing.app.FlinkAppArgumentsBuilder;
import org.apache.flink.operator.testing.app.FlinkDeploymentBuilder;
import org.apache.flink.operator.testing.properties.KafkaProperties;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.operator.testing.Constants.FLINK_IMAGE;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_ENTRYPOINT_HA;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_JAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
public class TestHaSavepoint extends TestBase {
    private static final String ID = new SimpleDateFormat("MM-dd-HH-mm").format(new Date());
    private static final String APP_NAME = "test-app-" + ID;
    private static final Integer TEST_DATA_COUNT = 10000;
    private static final String KAFKA_TOPIC_SOURCE = "sourceTopic-ha";
    private static final String KAFKA_TOPIC_SINK = "sinkTopic-ha";
    private final List<TestData> expectedResult = new ArrayList<>();

    @AfterEach
    public void after() {
        resourceManager
                .getResource(FlinkDeployment.class, APP_NAME)
                .ifPresent(resourceManager::deleteResource);
        kafkaClient.deleteTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));
    }

    public static Stream<Arguments> testHaLastState() {
        var deploymentModes =
                List.of(KubernetesDeploymentMode.NATIVE, KubernetesDeploymentMode.STANDALONE);
        var stateBackends = List.of("memory", "rocks", "hashmap");
        return Lists.cartesianProduct(deploymentModes, stateBackends).stream()
                .map(p -> Arguments.of(p.get(0), p.get(1)));
    }

    @ParameterizedTest
    @MethodSource
    public void testHaLastState(KubernetesDeploymentMode deploymentMode, String stateBackend) {
        kafkaClient.deleteTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));
        kafkaClient.createTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));

        produceRandomData();

        var flinkDeployment = createFlinkDeployment(deploymentMode, stateBackend);
        waitAtMost(Duration.ofMinutes(3)).untilAsserted(this::assertSinkResult);

        // Stop app
        resourceManager.editResource(
                flinkDeployment, d -> d.getSpec().getJob().setState(JobState.SUSPENDED));
        resourceManager.waitForResourceToBeInState(
                flinkDeployment, d -> "FINISHED".equals(d.getStatus().getJobStatus().getState()));
        produceRandomData();

        // Restart app
        resourceManager.editResource(
                flinkDeployment, d -> d.getSpec().getJob().setState(JobState.RUNNING));
        resourceManager.waitForAppToBeRunning(flinkDeployment);
        waitAtMost(Duration.ofMinutes(3)).untilAsserted(this::assertSinkResult);
    }

    private FlinkDeployment createFlinkDeployment(
            KubernetesDeploymentMode deploymentMode, String stateBackend) {
        var conf = minioCluster.getFlinkConfiguration();
        conf.setString(
                "high-availability",
                "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory");
        conf.setString("kubernetes.jobmanager.cpu.limit-factor", "1.3");
        conf.setString("kubernetes.taskmanager.cpu.limit-factor", "1.3");
        conf.setString("kubernetes.jobmanager.memory.limit-factor", "1.1");
        conf.setString("kubernetes.taskmanager.memory.limit-factor", "1.1");

        var args =
                new FlinkAppArgumentsBuilder()
                        .addProperties(
                                KafkaProperties.writeKafkaProperties(getFlinkKafkaProperties()))
                        .addProperty("kafka.source.topic", KAFKA_TOPIC_SOURCE)
                        .addProperty("kafka.sink.topic", KAFKA_TOPIC_SINK)
                        .addProperty("kafka.properties.auto.offset.reset", "earliest")
                        .addProperty("flink.state.backend", stateBackend)
                        .build();

        var jobSpec =
                JobSpec.builder()
                        .state(JobState.RUNNING)
                        .jarURI(TEST_JOB_JAR)
                        .entryClass(TEST_JOB_ENTRYPOINT_HA)
                        .args(args)
                        .upgradeMode(UpgradeMode.SAVEPOINT)
                        .parallelism(2)
                        .build();

        var result =
                new FlinkDeploymentBuilder()
                        .withAppName(APP_NAME)
                        .withFlinkImage(FLINK_IMAGE)
                        .withJobSpec(jobSpec)
                        .withKubernetesDeploymentMode(deploymentMode)
                        .withJobManagerReplicas(3)
                        .withTaskManagerReplicas(2)
                        .withFlinkConfiguration(conf.toMap())
                        .build();

        return resourceManager.waitForAppToBeRunning(resourceManager.createResource(result));
    }

    private void produceRandomData() {
        var testRecords =
                IntStream.range(expectedResult.size(), expectedResult.size() + TEST_DATA_COUNT)
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
