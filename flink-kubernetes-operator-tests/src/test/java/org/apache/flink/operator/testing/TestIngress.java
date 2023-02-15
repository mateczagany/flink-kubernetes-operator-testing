package org.apache.flink.operator.testing;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.IngressSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.operator.testing.app.FlinkDeploymentBuilder;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.operator.testing.Constants.FLINK_IMAGE;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_ENTRYPOINT_NOOP;
import static org.apache.flink.operator.testing.Constants.TEST_JOB_JAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
public class TestIngress extends TestBase {
    private static final String APP_NAME = "test-app";

    @AfterEach
    public void after() {
        resourceManager
                .getResource(FlinkDeployment.class, APP_NAME)
                .ifPresent(resourceManager::deleteResource);
    }

    @Test
    public void testIngress() {
        var flinkDeployment = createFlinkDeployment();
        waitAtMost(Duration.ofMinutes(30)).untilAsserted(this::assertRestEndpoint);
    }

    private FlinkDeployment createFlinkDeployment() {
        var conf = minioCluster.getFlinkConfiguration();

        var jobSpec =
                JobSpec.builder()
                        .state(JobState.RUNNING)
                        .jarURI(TEST_JOB_JAR)
                        .entryClass(TEST_JOB_ENTRYPOINT_NOOP)
                        .upgradeMode(UpgradeMode.SAVEPOINT)
                        .parallelism(2)
                        .build();

        var ingressSpec =
                IngressSpec.builder()
                        .className("nginx")
                        .template("flink.k8s.io/{{namespace}}/{{name}}(/|$)(.*)")
                        .annotations(Map.of("nginx.ingress.kubernetes.io/rewrite-target", "/$2"))
                        .build();

        var result =
                new FlinkDeploymentBuilder()
                        .withAppName(APP_NAME)
                        .withFlinkImage(FLINK_IMAGE)
                        .withJobSpec(jobSpec)
                        .withIngressSpec(ingressSpec)
                        .withKubernetesDeploymentMode(KubernetesDeploymentMode.NATIVE)
                        .withFlinkConfiguration(conf.toMap())
                        .build();

        return resourceManager.waitForAppToBeRunning(resourceManager.createResource(result));
    }

    private void assertRestEndpoint() {
        // TODO
        assertThat(true).isEqualTo(true);
    }
}
