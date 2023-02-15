package org.apache.flink.operator.testing.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.operator.testing.kubernetes.KubernetesConnector;
import org.apache.flink.kubernetes.operator.api.spec.JobState;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.waitAtMost;

@Slf4j
public class FlinkAppManager implements AutoCloseable {
    private final KubernetesConnector kubernetesConnector;
    private final FlinkAppStatsFetcher flinkAppStatsFetcher;

    public FlinkAppManager(KubernetesConnector kubernetesConnector) {
        this.kubernetesConnector = kubernetesConnector;
        this.flinkAppStatsFetcher = new FlinkAppStatsFetcher(kubernetesConnector);
    }

    @Override
    public void close() {
        kubernetesConnector.close();
    }

    public Map<String, String> getLogs(String appName) {
        return kubernetesConnector.getLogsForAllPods(appName);
    }

    public void createFlinkDeployment(FlinkDeployment flinkDeployment) {
        var appName = flinkDeployment.getMetadata().getName();
        log.info("Submitting FlinkDeployment {}", appName);

        kubernetesConnector.submitFlinkDeployment(flinkDeployment);
        waitForAppToBeRunning(appName);
    }

    public void restartApp(String appName) {
        var flinkDeployment = kubernetesConnector.getFlinkDeployment(appName)
                .orElseThrow(() -> new RuntimeException(String.format("App %s is not deployed", appName)));

        if (flinkDeployment.getSpec().getJob().getState().equals(JobState.RUNNING)) {
            throw new RuntimeException(String.format("App %s is already in running state", appName));
        }

        flinkDeployment.getSpec().getJob().setState(JobState.RUNNING);
        kubernetesConnector.patchFlinkDeployment(flinkDeployment);
        waitForAppToBeRunning(appName);
    }

    public void stopApp(String appName) {
        var flinkDeployment = kubernetesConnector.getFlinkDeployment(appName)
                .orElseThrow(() -> new RuntimeException(String.format("App %s is not deployed", appName)));

        if (flinkDeployment.getSpec().getJob().getState().equals(JobState.SUSPENDED)) {
            throw new RuntimeException(String.format("App %s is already stopped", appName));
        }

        var appStats = flinkAppStatsFetcher.fetch(appName);
        if (appStats == null) {
            throw new RuntimeException(String.format("App '%s' was not found", appName));
        }
        if (!appStats.getJobStatus().equals("RUNNING")) {
            throw new RuntimeException(String.format("App job state is %s", appStats.getJobStatus()));
        }
        if (!appStats.getJobManagerDeploymentStatus().equals("READY")) {
            throw new RuntimeException(String.format("App job manager state is %s", appStats.getJobManagerDeploymentStatus()));
        }

        flinkDeployment.getSpec().getJob().setState(JobState.SUSPENDED);
        kubernetesConnector.patchFlinkDeployment(flinkDeployment);
        waitForAppToBeStopped(appName);
    }

    public Optional<FlinkDeployment> getFlinkDeployment(String appName) {
        return kubernetesConnector.getFlinkDeployment(appName);
    }

    public void deleteAppIfPresent(String appName) {
        kubernetesConnector.getFlinkDeployment(appName).ifPresent(kubernetesConnector::deleteFlinkDeployment);
    }

    public Map<String, Integer> getParallelismOverridesForJob(String jobName) {
        var configMapOpt = kubernetesConnector.getConfigMap("autoscaler-" + jobName);
        if (configMapOpt.isEmpty() || !configMapOpt.get().containsKey("parallelismOverrides")) {
            return Map.of();
        }

        return Arrays.stream(configMapOpt.get().get("parallelismOverrides").split(","))
                .map(s -> s.split(":"))
                .filter(split -> split.length == 2)
                .collect(Collectors.toMap(split -> split[0], split -> Integer.valueOf(split[1])));
    }

    private FlinkAppStats waitForAppToBeRunning(String appName) {
        log.info("Waiting for '{}' reconciliation to be done", appName);
        var appStats = waitForAppToBeInState(appName, s -> "DEPLOYED".equals(s.getReconciliationState()));

        log.info("Waiting for '{}' job manager to be ready", appName);
        appStats = waitForAppToBeInState(appName, s ->
                Set.of("READY", "MISSING", "ERROR").contains(s.getJobManagerDeploymentStatus()));

        if (!appStats.getJobManagerDeploymentStatus().equals("READY")) {
            var logs = kubernetesConnector.getJobManagerLogs(appName);
            log.error("Job manager failed to start, state: {}, pod logs:\n{}", appStats.getJobManagerDeploymentStatus(), logs);
            throw new RuntimeException(String.format("Job manager failed to start, last state: %s", appStats.getJobManagerDeploymentStatus()));
        }

        log.info("Waiting for '{}' job to be RUNNING", appName);
        appStats = waitForAppToBeInState(appName, s ->
                Set.of("RUNNING", "FAILING", "FAILED", "FINISHED").contains(s.getJobStatus()));

        if (!appStats.getJobStatus().equals("RUNNING") && !appStats.getJobStatus().equals("FINISHED")) {
            var logs = kubernetesConnector.getJobManagerLogs(appName);
            log.error("Job failed to start, state: {}, pod logs:\n{}", appStats.getJobStatus(), logs);
            throw new RuntimeException(String.format("Job failed to start, last state: %s", appStats.getJobStatus()));
        }

        log.info("Status: {}", appStats.getJobStatus());
        return appStats;
    }

    private void waitForAppToBeStopped(String appName) {
        log.info("Waiting for '{}' job manager to be destroyed", appName);
        waitForAppToBeInState(appName,
                appStats -> appStats.getJobStatus().equals("FINISHED"));
    }

    private FlinkAppStats waitForAppToBeInState(String appName,
                                                Predicate<FlinkAppStats> predicate) {
        return waitAtMost(Duration.ofMinutes(3))
                .pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> flinkAppStatsFetcher.fetch(appName), predicate);
    }
}
