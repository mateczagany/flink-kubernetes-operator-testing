package org.apache.flink.operator.testing;

import com.dajudge.kindcontainer.KindContainer;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.operator.testing.app.FlinkAppManager;
import org.apache.flink.operator.testing.kafka.KafkaClient;
import org.apache.flink.operator.testing.kubernetes.KubernetesConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TestBase {
    protected final static TestConfig CONFIG = TestConfig.load();
    private final Network containerNetwork = Network.newNetwork();
    private final KindContainer<?> kindContainer = new KindContainer<>()
            .withLogConsumer(new Slf4jLogConsumer(log))
            .withKubectl(kubectl -> {
                kubectl.apply.from("https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml").run();
                kubectl.wait
                        .namespace("cert-manager")
                        .timeout("600s")
                        .forCondition("Available")
                        .run("deployment", "--all");
            })
            .withHelm3(helm -> {
                helm.install
                        .namespace("flink")
                        .createNamespace()
                        .set("replicas", "3")
                        .set("defaultConfiguration.create", "true")
                        .set("defaultConfiguration.append", "true")
                        .set("defaultConfiguration.flink-conf\\.yaml", "kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory\n" +
                                "kubernetes.operator.metrics.reporter.slf4j.interval: 1 MINUTE\n" +
                                "kubernetes.operator.reconcile.interval: 10 s\n" +
                                "kubernetes.operator.observer.progress-check.interval: 10 s\n" +
                                "kubernetes.operator.leader-election.enabled: true\n" +
                                "kubernetes.operator.leader-election.lease-name: flink-operator-lease")
                        .set("defaultConfiguration.log4j-operator\\.properties", "rootLogger.level = DEBUG\n" +
                                "logger.operator.name= org.apache.flink.kubernetes.operator\n" +
                                "logger.operator.level = DEBUG")
                        .set("image.repository", "czmate10/flink-kubernetes-operator")
                        .set("image.tag", "latest")
                        .run("flink-kubernetes-operator", "~/dev/repos/flink-kubernetes-operator/helm/flink-kubernetes-operator");
            })
            .withKubectl(kubectl -> {
                kubectl.wait
                        .namespace("flink")
                        .timeout("600s")
                        .forCondition("Available")
                        .run("deployment", "--all");
            })
            .withNetwork(containerNetwork);
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"))
            .withEmbeddedZookeeper()
            .withNetwork(containerNetwork);
    protected FlinkAppManager flinkAppManager = null;
    protected KafkaClient kafkaClient = null;

    protected Map<String, String> getFlinkKafkaProperties() {
        var result = new HashMap<String, String>();
        result.put("transaction.timeout.ms", "120000");

        if (CONFIG.getKafkaProperties() == null || CONFIG.getKafkaProperties().isEmpty()) {
            result.put("bootstrap.servers", kafkaContainer.getNetworkAliases().get(0) + ":9092");
        } else {
            result.putAll(CONFIG.getKafkaProperties());
        }

        return result;
    }

    @BeforeEach
    public void createFlinkAppManager() {
        KubernetesClient kubernetesClient;
        if (CONFIG.getCreateKubernetesCluster()) {
            kindContainer.start();
            kubernetesClient = new KubernetesClientBuilder()
                    .withConfig(Config.fromKubeconfig(kindContainer.getKubeconfig()))
                    .build();

            try {
                Files.write(Path.of("/tmp/flink.kube.config"), kindContainer.getKubeconfig().getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            kubernetesClient = new KubernetesClientBuilder().build();
        }

        kubernetesClient.getConfiguration().setNamespace(Constants.KUBERNETES_NAMESPACE);
        flinkAppManager = new FlinkAppManager(new KubernetesConnector(kubernetesClient));
    }

    @BeforeEach
    public void createKafkaClient() {
        if (CONFIG.getKafkaProperties() == null || CONFIG.getKafkaProperties().isEmpty()) {
            kafkaContainer.start();
            kafkaClient = new KafkaClient(Map.of("bootstrap.servers", "localhost:" + kafkaContainer.getMappedPort(9093)));
        } else {
            kafkaClient = new KafkaClient(CONFIG.getKafkaProperties());
        }
    }

    @AfterEach
    public void cleanup() {
        if (flinkAppManager != null) {
            flinkAppManager.close();
        }
        if (kindContainer != null && kindContainer.isRunning()) {
            kindContainer.close();
        }
        if (kafkaContainer != null && kafkaContainer.isRunning()) {
            kafkaContainer.close();
        }
    }
}
