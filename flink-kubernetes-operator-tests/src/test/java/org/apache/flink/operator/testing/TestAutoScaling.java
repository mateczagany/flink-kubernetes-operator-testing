package org.apache.flink.operator.testing;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.operator.testing.app.FlinkAppArgumentsBuilder;
import org.apache.flink.operator.testing.app.FlinkDeploymentBuilder;
import org.apache.flink.operator.testing.properties.KafkaProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Map.entry;
import static org.apache.flink.operator.testing.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;


@Slf4j
public class TestAutoScaling extends TestBase {
    private static final String APP_NAME = "test-app-scaling";
    private static final String KAFKA_TOPIC_SOURCE = "sourceTopic";
    private static final String KAFKA_TOPIC_SINK = "sinkTopic";
    private final List<TestData> expectedResult = new ArrayList<>();

    @AfterEach
    public void after() {
        flinkAppManager.deleteAppIfPresent(APP_NAME);
        kafkaClient.deleteTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));
    }

    @Test
    public void testAutoScaling() {
        flinkAppManager.deleteAppIfPresent(APP_NAME);
        kafkaClient.deleteTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));
        kafkaClient.createTopics(List.of(KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_SINK));
        produceRandomData(1, 0);

        flinkAppManager.createFlinkDeployment(createFlinkDeployment());

        // First produce enough data to trigger up-scaling
        // Assert that up-scaling happened
        // Then assert that down-scaling happened

        // 1m stabilization.interval 1m metrics.window
        int defaultParallelism = 4;
        long interval = 1000L;
        double targetUtilization = 1.0;

        try (var kafkaRecordWriter = kafkaClient.writer(KAFKA_TOPIC_SOURCE)) {
            waitAtMost(Duration.ofMinutes(10))
                    .pollInSameThread()
                    .pollInterval(Duration.ofMillis(interval))
                    .untilAsserted(() -> {
                        var testRecords = IntStream.range(0, defaultParallelism)
                                .mapToObj(i -> TestData.generateRandom(expectedResult.size() + i, (long) (interval * targetUtilization)))
                                .collect(Collectors.toList());
                        expectedResult.addAll(testRecords);
                        kafkaRecordWriter.writeRecords(testRecords);
                        assertFlinkDeploymentParallelismIs(6);
                    });
        }

        // Produce more data to make sure job restarts and picks up last state well
        produceRandomData(100, 0);

        waitAtMost(Duration.ofMinutes(20))
                .pollInterval(Duration.ofSeconds(5))
                .untilAsserted(this::assertSinkResult);
    }

    private FlinkDeployment createFlinkDeployment() {
        var flinkConfiguration = Map.ofEntries(
                entry("state.savepoints.dir", "file:///flink-data/savepoints"),
                entry("state.checkpoints.dir", "file:///flink-data/checkpoints"),
                entry("high-availability", "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory"),
                entry("high-availability.storageDir", "file:///flink-data/ha"),

                entry("kubernetes.operator.job.autoscaler.enabled", "true"),
                entry("kubernetes.operator.job.autoscaler.target.utilization", "0.6"),
                entry("kubernetes.operator.job.autoscaler.target.utilization.boundary", "0.1"),
                entry("kubernetes.operator.job.autoscaler.stabilization.interval", "60s"),
                entry("kubernetes.operator.job.autoscaler.metrics.window", "60s"),
                entry("kubernetes.operator.job.autoscaler.metrics.busy-time.aggregator", "AVG"),
                entry("kubernetes.operator.job.autoscaler.restart.time", "0s"),
                entry("kubernetes.operator.job.autoscaler.catch-up.duration", "0s"),

                entry("pipeline.max-parallelism", "12"),
                entry("taskmanager.numberOfTaskSlots", "4"),
                entry("jobmanager.scheduler", "adaptive")
        );

        var args = new FlinkAppArgumentsBuilder()
                .addProperties(KafkaProperties.writeKafkaProperties(getFlinkKafkaProperties()))
                .addProperty("kafka.source.topic", KAFKA_TOPIC_SOURCE)
                .addProperty("kafka.sink.topic", KAFKA_TOPIC_SINK)
                .addProperty("transactional.id.prefix", UUID.randomUUID().toString())
                .addProperty("kafka.properties.auto.offset.reset", "earliest")
                .build();

        var jobSpec = JobSpec.builder()
                .state(JobState.RUNNING)
                .jarURI(TEST_JOB_JAR)
                .entryClass(TEST_JOB_ENTRYPOINT_AUTO_SCALING)
                .args(args)
                .upgradeMode(UpgradeMode.LAST_STATE)
                .parallelism(4)
                .build();

        return new FlinkDeploymentBuilder()
                .withAppName(APP_NAME)
                .withFlinkImage("czmate10/flink-test-app:latest")
                .withJobSpec(jobSpec)
                .withKubernetesDeploymentMode(KubernetesDeploymentMode.NATIVE)
                .withJobManagerCpu(1D)
                .withJobManagerMemory("2048m")
                .withTaskManagerCpu(0.5D)
                .withTaskManagerMemory("1024m")
                .withFlinkConfiguration(flinkConfiguration)
                .build();
    }

    private void produceRandomData(int testDataCount, long sleepMillis) {
        var testRecords = IntStream.range(expectedResult.size(), expectedResult.size() + testDataCount)
                .mapToObj(i -> TestData.generateRandom(i, sleepMillis))
                .collect(Collectors.toList());
        expectedResult.addAll(testRecords);

        try (var kafkaRecordWriter = kafkaClient.writer(KAFKA_TOPIC_SOURCE)) {
            kafkaRecordWriter.writeRecords(testRecords);
        }
    }

    private void assertSinkResult() {
        try (var kafkaRecordReader = kafkaClient.reader(KAFKA_TOPIC_SINK)) {
            var records = kafkaRecordReader.readRecords();
            log.info("Found {} records", records.size());
            assertThat(records).containsAll(expectedResult);
        }
    }

    private void assertFlinkDeploymentParallelismIs(int minParallelism) {
        var overrides = flinkAppManager.getParallelismOverridesForJob(APP_NAME);

        assertThat(overrides).isNotEmpty();

        log.info("Found JobVertex overrides: {}", overrides);

        var mapParallelism = overrides.values().stream()
                .filter(e -> e != 1)
                .collect(Collectors.toList());

        assertThat(mapParallelism).hasSize(1);
        assertThat(mapParallelism.get(0)).isGreaterThanOrEqualTo(minParallelism);
    }
}
