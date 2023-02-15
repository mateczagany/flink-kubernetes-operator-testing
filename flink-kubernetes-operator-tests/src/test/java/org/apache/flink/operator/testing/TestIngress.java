package org.apache.flink.operator.testing;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.operator.testing.app.FlinkAppArgumentsBuilder;
import org.apache.flink.operator.testing.app.FlinkDeploymentBuilder;
import org.apache.flink.operator.testing.properties.KafkaProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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

import static java.util.Map.entry;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_ENTRYPOINT_HA;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_ENTRYPOINT_NOOP;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_JAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;


@Slf4j
public class TestIngress extends TestBase {
    private static final String APP_NAME = "test-app";

    @AfterEach
    public void after() {
        flinkAppManager.deleteAppIfPresent(APP_NAME);
    }

    @Test
    public void testIngress() {
        flinkAppManager.deleteAppIfPresent(APP_NAME);

        flinkAppManager.createFlinkDeployment(getFlinkDeployment());
        waitAtMost(Duration.ofMinutes(30)).untilAsserted(this::assertRestEndpoint);
    }

    private FlinkDeployment getFlinkDeployment() {
        var flinkConfig = Map.of(
                "state.savepoints.dir", "file:///flink-data/savepoints",
                "state.checkpoints.dir", "file:///flink-data/checkpoints"
        );

        var jobSpec = JobSpec.builder()
                .state(JobState.RUNNING)
                .jarURI(TEST_JOB_JAR)
                .entryClass(TEST_JOB_ENTRYPOINT_NOOP)
                .upgradeMode(UpgradeMode.SAVEPOINT)
                .parallelism(2)
                .build();

        var ingressSpec = IngressSpec.builder()
                .className("nginx")
                .template("flink.k8s.io/{{namespace}}/{{name}}(/|$)(.*)")
                .annotations(Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"))
                .build();

        return new FlinkDeploymentBuilder()
                .withAppName(APP_NAME)
                .withFlinkImage("czmate10/flink-test-app:latest")
                .withJobSpec(jobSpec)
                .withIngressSpec(ingressSpec)
                .withKubernetesDeploymentMode(KubernetesDeploymentMode.NATIVE)
                .withFlinkConfiguration(flinkConfig)
                .build();
    }

    private void assertRestEndpoint() {
        // TODO
        assertThat(true).isEqualTo(true);
    }
}
