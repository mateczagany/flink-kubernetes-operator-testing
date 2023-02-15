package org.apache.flink.operator.testing;

import org.apache.flink.operator.testing.kafka.KafkaClient;
import org.apache.flink.operator.testing.kubernetes.resource.AbstractKubernetesService;
import org.apache.flink.operator.testing.kubernetes.resource.KafkaCluster;
import org.apache.flink.operator.testing.kubernetes.resource.MinioCluster;
import org.apache.flink.operator.testing.kubernetes.resource.ResourceManager;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class TestBase {

    protected static final TestConfig CONFIG = TestConfig.load();
    protected ResourceManager resourceManager = null;
    protected MinioCluster minioCluster = null;
    protected KafkaCluster kafkaCluster = null;
    protected KafkaClient kafkaClient = null;

    protected Map<String, String> getFlinkKafkaProperties() {
        var result = new HashMap<String, String>();
        result.put("transaction.timeout.ms", "120000");
        result.put("bootstrap.servers", kafkaCluster.getBootstrapServers());

        return result;
    }

    @BeforeEach
    public void beforeEach() {
        KubernetesClient kubernetesClient;
        kubernetesClient = new KubernetesClientBuilder().build();

        kubernetesClient.getConfiguration().setNamespace(Constants.KUBERNETES_NAMESPACE);
        resourceManager = new ResourceManager(kubernetesClient);

        minioCluster = new MinioCluster(resourceManager);
        kafkaCluster = new KafkaCluster(resourceManager);
        kafkaClient =
                new KafkaClient(Map.of("bootstrap.servers", kafkaCluster.getBootstrapServers()));
    }

    @SneakyThrows
    @AfterEach
    public void cleanup() {
        Optional.ofNullable(minioCluster).ifPresent(AbstractKubernetesService::close);
        Optional.ofNullable(kafkaCluster).ifPresent(AbstractKubernetesService::close);
        Optional.ofNullable(resourceManager).ifPresent(ResourceManager::close);
    }
}
