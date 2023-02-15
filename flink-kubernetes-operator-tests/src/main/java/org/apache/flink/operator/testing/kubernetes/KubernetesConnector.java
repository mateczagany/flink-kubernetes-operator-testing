package org.apache.flink.operator.testing.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.operator.testing.Constants;
import org.awaitility.core.ConditionEvaluationLogger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
@AllArgsConstructor
public class KubernetesConnector implements AutoCloseable {
    @Getter
    private final KubernetesClient kubernetesClient;

    @Override
    public void close() {
        kubernetesClient.close();
    }

    public void submitFlinkDeployment(FlinkDeployment flinkDeployment) {
        String appName = flinkDeployment.getMetadata().getName();
        log.info("Submitting FlinkDeployment '{}'", appName);

        kubernetesClient.resources(PersistentVolumeClaim.class)
                .resource(new PersistentVolumeClaimBuilder()
                        .withMetadata(new ObjectMetaBuilder().withName(appName).build())
                        .withSpec(new PersistentVolumeClaimSpecBuilder()
                                .withAccessModes("ReadWriteOnce")
                                .withVolumeMode("Filesystem")
                                .withResources(new ResourceRequirements(List.of(), Map.of("storage", new Quantity("1", "Gi")), Map.of("storage", new Quantity("1", "Gi"))))
                                .build())
                        .build())
                .serverSideApply();

        kubernetesClient.resources(FlinkDeployment.class)
                .resource(flinkDeployment)
                .createOrReplace();
    }

    public void patchFlinkDeployment(FlinkDeployment flinkDeployment) {
        log.info("Editing FlinkDeployment '{}'", flinkDeployment.getMetadata().getName());

        kubernetesClient.resources(FlinkDeployment.class)
                .resource(flinkDeployment)
                .patch(PatchContext.of(PatchType.JSON_MERGE), flinkDeployment);
    }

    public void deleteFlinkDeployment(FlinkDeployment flinkDeployment) {
        var appName = flinkDeployment.getMetadata().getName();
        log.info("Deleting FlinkDeployment {}", appName);

        kubernetesClient.resources(FlinkDeployment.class)
                .resource(flinkDeployment)
                .delete();

        kubernetesClient.resources(PersistentVolumeClaim.class)
                .withName(appName)
                .delete();

        waitAtMost(Duration.ofMinutes(1))
                .conditionEvaluationListener(new ConditionEvaluationLogger(log::info))
                .pollDelay(Duration.ofMillis(200))
                .until(() -> getFlinkDeployment(appName), Optional::isEmpty);
    }

    public Optional<Map<String, String>> getConfigMap(String configMapName) {
        return Optional.ofNullable(kubernetesClient
                        .configMaps()
                        .withName(configMapName)
                        .get())
                .map(ConfigMap::getData);
    }

    public String getJobManagerLogs(String appName) {
        return getJobManagePodForFlinkDeployment(appName)
                .map(this::getPodLogs).orElse("");
    }

    public Map<String, String> getLogsForAllPods(String appName) {
        return getPodsForFlinkDeployment(appName).stream()
                .collect(Collectors.toMap(pod -> pod.getMetadata().getName(),
                        this::getPodLogs));
    }

    public Optional<FlinkDeployment> getFlinkDeployment(String appName) {
        return Optional.ofNullable(kubernetesClient.resources(FlinkDeployment.class)
                .withName(appName)
                .get());
    }

    private String getPodLogs(Pod pod) {
        return waitAtMost(Duration.ofMinutes(1))
                .conditionEvaluationListener(new ConditionEvaluationLogger(log::info))
                .pollDelay(Duration.ofMillis(200))
                .until(
                        () -> kubernetesClient.pods().resource(pod).inContainer(Constants.KUBERNETES_CONTAINER_FLINK_NAME).getLog(true),
                        r -> r.contains("Starting ")
                );
    }

    private List<Pod> getPodsForFlinkDeployment(String appName) {
        return kubernetesClient.pods().withLabel("app", appName).list().getItems();
    }

    private Optional<Pod> getJobManagePodForFlinkDeployment(String appName) {
        return getPodsForFlinkDeployment(appName).stream()
                .filter(d -> d.getMetadata().getLabels().getOrDefault("component", "").equals("jobmanager"))
                .findFirst();
    }
}
