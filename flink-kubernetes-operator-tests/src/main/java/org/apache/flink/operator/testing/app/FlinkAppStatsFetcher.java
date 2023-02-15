package org.apache.flink.operator.testing.app;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.operator.testing.kubernetes.KubernetesConnector;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.utils.SpecUtils;

import static org.awaitility.Awaitility.waitAtMost;

@AllArgsConstructor
@Slf4j
public class FlinkAppStatsFetcher {
    private final KubernetesConnector kubernetesConnector;

    public FlinkAppStats fetch(String appName) {
        var flinkDeployment = kubernetesConnector.getFlinkDeployment(appName)
                .orElseThrow(() -> new RuntimeException(String.format("App %s is not deployed", appName)));

        var lastReconciledSpec = flinkDeployment.getStatus().getReconciliationStatus().getLastReconciledSpec();
        var lastReconciledGeneration = SpecUtils.deserializeSpecWithMeta(lastReconciledSpec, FlinkDeploymentSpec.class)
                .getMeta().getMetadata().getGeneration();
        if (!lastReconciledGeneration.equals(flinkDeployment.getMetadata().getGeneration())) {
            throw new RuntimeException("Kubernetes Operator has not reconciled this generation yet");
        }

        var jobStatus = flinkDeployment.getStatus().getJobStatus();
        var clusterInfo = flinkDeployment.getStatus().getClusterInfo();
        var jobManagerDeploymentStatus = flinkDeployment.getStatus().getJobManagerDeploymentStatus();
        var taskManagerInfo = flinkDeployment.getStatus().getTaskManager();
        var reconciliationState = flinkDeployment.getStatus().getReconciliationStatus().getState();
        var statusError = flinkDeployment.getStatus().getError();

        return FlinkAppStats.builder()
                .flinkVersion(clusterInfo.getOrDefault("flink-version", "Unknown"))
                .jobId(jobStatus.getJobId())
                .jobStatus(jobStatus.getState())
                .jobManagerDeploymentStatus(jobManagerDeploymentStatus.toString())
                .lastSavepointTimestamp(jobStatus.getSavepointInfo().getLastPeriodicSavepointTimestamp())
                .taskManagerCount(taskManagerInfo != null ? taskManagerInfo.getReplicas() : 0)
                .reconciliationState(reconciliationState.toString())
                .error(statusError)
                .build();
    }
}
