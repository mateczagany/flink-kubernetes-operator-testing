package org.apache.flink.operator.testing.kubernetes.resource;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpecBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class KafkaCluster extends AbstractKubernetesService {

    private static final String IMAGE = "apache/kafka:3.7.1";
    private int port;

    public KafkaCluster(ResourceManager resourceManager) {
        super(resourceManager);
    }

    @Override
    protected void initialize() {
        port = findRandomOpenPort();
    }

    @Override
    protected String getNamePrefix() {
        return "kafka";
    }

    @Override
    protected List<HasMetadata> createResources() {
        return List.of(createPod());
    }

    @Override
    protected List<Service> createPortForwardedServices() {
        return List.of(createService());
    }

    public String getBootstrapServers() {
        return String.format("%s:%d", getHostname(), port);
    }

    private Pod createPod() {
        var metadata =
                new ObjectMetaBuilder().withName(name).withLabels(getSelectorLabels()).build();

        var envVarsMap =
                Map.ofEntries(
                        Map.entry("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw"),
                        Map.entry(
                                "KAFKA_LISTENERS",
                                String.format(
                                        "PLAINTEXT://:%d,BROKER://:9093,CONTROLLER://:9094", port)),
                        Map.entry(
                                "KAFKA_ADVERTISED_LISTENERS",
                                String.format(
                                        "PLAINTEXT://%s:%d,BROKER://:9093", getHostname(), port)),
                        Map.entry(
                                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"),
                        Map.entry("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
                        Map.entry("KAFKA_PROCESS_ROLES", "broker,controller"),
                        Map.entry("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
                        Map.entry("KAFKA_NODE_ID", "1"),
                        Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1"),
                        Map.entry("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1"),
                        Map.entry("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1"),
                        Map.entry("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "9223372036854775807"),
                        Map.entry("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
                        Map.entry("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094"));
        var envVars =
                envVarsMap.entrySet().stream()
                        .map(e -> new EnvVar(e.getKey(), e.getValue(), null))
                        .collect(Collectors.toList());

        var container =
                new ContainerBuilder()
                        .withName("kafka")
                        .withImage(IMAGE)
                        .withPorts(new ContainerPortBuilder().withContainerPort(port).build())
                        .withEnv(envVars)
                        .build();

        var spec = new PodSpecBuilder().withContainers(container).build();
        var pod = new PodBuilder().withMetadata(metadata).withSpec(spec).build();

        return resourceManager.createResource(pod);
    }

    private Service createService() {
        var metadata = new ObjectMetaBuilder().withName(name).build();
        var spec =
                new ServiceSpecBuilder()
                        .withSelector(getSelectorLabels())
                        .withPorts(
                                new ServicePortBuilder().withProtocol("TCP").withPort(port).build())
                        .build();

        var service = new ServiceBuilder().withMetadata(metadata).withSpec(spec).build();

        return resourceManager.createResource(service);
    }
}
