package org.apache.flink.operator.testing;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.*;
import org.apache.flink.operator.testing.app.FlinkDeploymentBuilder;
import org.apache.flink.operator.testing.kubernetes.resource.MinioCluster;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.Nullable;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.configuration.WebOptions.CHECKPOINTS_HISTORY_SIZE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.ABANDONED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.COMPLETED;
import static org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus.State.FAILED;
import static org.apache.flink.operator.testing.Constants.FLINK_IMAGE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static org.junit.Assert.assertThrows;

@Slf4j
public class TestSnapshots extends TestBase {
    private static final String ID = new SimpleDateFormat("MM-dd-HH-mm").format(new Date());
    private static final String APP_NAME = "test-snapshots-job-" + ID;
    private static final String CUSTOM_PATH =
            String.format("s3://%s/savepoints-custom", MinioCluster.BUCKET_NAME);
    private final Configuration DEFAULT_JOB_CONF =
            new Configuration()
                    .set(CHECKPOINTING_INTERVAL, Duration.ofSeconds(1))
                    .set(INCREMENTAL_CHECKPOINTS, true)
                    .set(MAX_RETAINED_CHECKPOINTS, 1000)
                    .set(CHECKPOINTS_HISTORY_SIZE, 1000);

    private AbstractFlinkResource<?, ?> flinkResource = null;
    private JobReference jobRef = null;

    @AfterEach
    public void after() {
        if (jobRef != null) {
            resourceManager.getResources(FlinkStateSnapshot.class).stream()
                    .filter(s -> jobRef.equals(s.getSpec().getJobReference()))
                    .forEach(resourceManager::deleteResource);
        }
        Optional.ofNullable(flinkResource).ifPresent(resourceManager::deleteResource);
        if (flinkResource instanceof FlinkSessionJob) {
            var flinkDeploymentName =
                    ((FlinkSessionJob) flinkResource).getSpec().getDeploymentName();
            resourceManager
                    .getResource(FlinkDeployment.class, flinkDeploymentName)
                    .ifPresent(resourceManager::deleteResource);
        }

        resourceManager.waitForResourceToBeInState(flinkResource, Objects::isNull);
    }

    @Test
    public void testSnapshotValidation() {
        // JobReference is null
        jobRef = null;
        var ex = assertThrows(KubernetesClientException.class, () -> createSavepointSnapshot(null));
        assertThat(ex).hasMessageContaining("Job reference must be supplied for this snapshot");

        // JobReference points to not existing job
        jobRef =
                new JobReference(
                        JobKind.FLINK_DEPLOYMENT, "non-existent", Constants.KUBERNETES_NAMESPACE);
        ex = assertThrows(KubernetesClientException.class, () -> createSavepointSnapshot(null));
        assertThat(ex)
                .hasMessageContaining(
                        String.format(
                                "Target for snapshot %s/%s in namespace was not found",
                                jobRef.getNamespace(), jobRef.getName()));

        // Snapshot sets both savepoint and checkpoint spec
        var snapshot = new FlinkStateSnapshot();
        snapshot.setMetadata(
                new ObjectMetaBuilder()
                        .withName("test-savepoint-" + UUID.randomUUID().toString().substring(0, 8))
                        .build());
        snapshot.setSpec(
                FlinkStateSnapshotSpec.builder()
                        .jobReference(jobRef)
                        .savepoint(new SavepointSpec())
                        .checkpoint(new CheckpointSpec())
                        .build());
        ex =
                assertThrows(
                        KubernetesClientException.class,
                        () -> resourceManager.createResource(snapshot));
        assertThat(ex)
                .hasMessageContaining(
                        "Exactly one of checkpoint or savepoint configurations has to be set.");
    }

    @Test
    public void testSnapshotAbandon() {
        flinkResource = createFlinkDeployment(false, false);
        jobRef = JobReference.fromFlinkResource(flinkResource);

        var snapshot = createSavepointSnapshot(null);
        resourceManager.deleteResource(flinkResource);

        resourceManager.waitForResourceToBeInState(
                snapshot,
                s -> {
                    var state = s.getStatus().getState();
                    log.info(
                            "Waiting for snapshot {} to be abandoned, current: {}...",
                            s.getMetadata().getName(),
                            state.name());
                    return ABANDONED.equals(state);
                });
        resourceManager.deleteResource(snapshot);
        resourceManager.waitForResourceToBeInState(snapshot, Objects::isNull);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSnapshotsLegacy(boolean isSessionMode) {
        flinkResource = createFlinkDeployment(true, isSessionMode);

        var snapshots = resourceManager.getResources(FlinkStateSnapshot.class);

        assertManualSavepointNonce(true);

        var snapshots2 = resourceManager.getResources(FlinkStateSnapshot.class);
        assertThat(snapshots).containsExactlyInAnyOrderElementsOf(snapshots2);
    }

    @Test
    public void testJobReferenceOtherNamespace() {
        // Create job and trigger manual savepoint
        flinkResource = createFlinkDeployment(true, false);
        jobRef = JobReference.fromFlinkResource(flinkResource);

        var snapshot = new FlinkStateSnapshot();
        snapshot.setMetadata(
                new ObjectMetaBuilder()
                        .withName("test-savepoint-" + UUID.randomUUID().toString().substring(0, 8))
                        .withNamespace("default")
                        .build());
        snapshot.setSpec(
                FlinkStateSnapshotSpec.builder()
                        .jobReference(JobReference.fromFlinkResource(flinkResource))
                        .savepoint(SavepointSpec.builder().disposeOnDelete(true).build())
                        .build());
        resourceManager.createResource(snapshot);
        snapshot =
                resourceManager.waitForResourceToBeInState(
                        snapshot, s -> COMPLETED.equals(s.getStatus().getState()));
        log.info("Savepoint path: {}", snapshot.getStatus().getPath());
        resourceManager.deleteResource(snapshot);
    }

    @Test
    public void testJobStartsFromSnapshot() {
        // Create job and trigger manual savepoint
        flinkResource = createFlinkDeployment(true, false);
        jobRef = JobReference.fromFlinkResource(flinkResource);
        var path = assertManualSavepointNonce(true);

        var snapshot = new FlinkStateSnapshot();
        snapshot.setMetadata(
                new ObjectMetaBuilder()
                        .withName("test-savepoint-" + UUID.randomUUID().toString().substring(0, 8))
                        .withNamespace("default")
                        .build());
        snapshot.setSpec(
                FlinkStateSnapshotSpec.builder()
                        .savepoint(
                                SavepointSpec.builder()
                                        .path(path)
                                        .alreadyExists(true)
                                        .disposeOnDelete(true)
                                        .build())
                        .build());
        resourceManager.createResource(snapshot);

        // Resource reference
        resourceManager.deleteResource(flinkResource);
        resourceManager.waitForResourceToBeInState(flinkResource, Objects::isNull);
        flinkResource =
                createFlinkDeployment(
                        false, false, FlinkStateSnapshotReference.fromResource(snapshot));
        resourceManager.waitForAppToBeRunning(flinkResource);
        assertThat(resourceManager.getFlinkJobManagerPodLogs(APP_NAME))
                .contains(String.format("execution.savepoint.path, %s", path));

        // Path reference
        resourceManager.deleteResource(flinkResource);
        resourceManager.waitForResourceToBeInState(flinkResource, Objects::isNull);
        flinkResource =
                createFlinkDeployment(false, false, FlinkStateSnapshotReference.fromPath(path));
        resourceManager.waitForAppToBeRunning(flinkResource);
        assertThat(resourceManager.getFlinkJobManagerPodLogs(APP_NAME))
                .contains(String.format("execution.savepoint.path, %s", path));
    }

    @Test
    public void testCheckpointWithExpiredStats() {
        // Set web history size to 1, this should make the operator unable to pull the checkpoint
        // path.
        var conf = new Configuration().set(CHECKPOINTS_HISTORY_SIZE, 1);
        flinkResource = createFlinkDeployment(false, false, null, conf);
        jobRef = JobReference.fromFlinkResource(flinkResource);

        var checkpoint = createCheckpointSnapshot();
        checkpoint =
                resourceManager.waitForResourceToBeInState(
                        checkpoint, c -> COMPLETED.equals(c.getStatus().getState()));
        assertThat(checkpoint.getStatus().getPath()).isEqualTo("");
        assertThat(resourceManager.getResourceEvents(checkpoint))
                .hasSize(1)
                .allSatisfy(e -> assertThat(e.getMessage()).contains("failed to fetch path"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSnapshots(boolean isSessionMode) {
        flinkResource = createFlinkDeployment(false, isSessionMode);
        jobRef = JobReference.fromFlinkResource(flinkResource);

        // Manual savepoint with nonce
        assertManualSavepointNonce(false);

        // Stop with savepoint and restart
        assertUpgradeSavepoint();

        // Manual savepoint, but with incorrect custom path to test errors
        assertIncorrectManualSavepoint();

        // Manual savepoint with new FlinkStateSnapshot
        var snapshots = new ArrayList<FlinkStateSnapshot>();
        IntStream.range(0, 45).forEach(k -> snapshots.add(createSavepointSnapshot(null)));
        IntStream.range(0, 45).forEach(k -> snapshots.add(createSavepointSnapshot(CUSTOM_PATH)));
        IntStream.range(0, 25).forEach(k -> snapshots.add(createCheckpointSnapshot()));
        snapshots.forEach(
                snapshot -> {
                    assertManualSnapshot(snapshot);
                    resourceManager.deleteResource(snapshot);
                });
    }

    private FlinkStateSnapshot waitForSnapshot(@Nullable String snapshotType) {
        Predicate<FlinkStateSnapshot> filter =
                snapshot -> {
                    if (!jobRef.equals(snapshot.getSpec().getJobReference())) {
                        return false;
                    }
                    if (!snapshot.getMetadata()
                            .getLabels()
                            .get("snapshot.type")
                            .equals(snapshotType)) {
                        return false;
                    }
                    return snapshot.getStatus() != null
                            && COMPLETED.equals(snapshot.getStatus().getState());
                };

        var result =
                waitAtMost(Duration.ofMinutes(5))
                        .pollInterval(Duration.ofSeconds(5))
                        .until(
                                () ->
                                        resourceManager
                                                .getResources(FlinkStateSnapshot.class)
                                                .stream()
                                                .filter(filter)
                                                .findFirst(),
                                Optional::isPresent)
                        .orElseThrow();

        log.info("Found new {} snapshot {}", snapshotType, result.getMetadata().getName());
        return result;
    }

    private void assertIncorrectManualSavepoint() {
        var snapshot = createSavepointSnapshot("s3://NON-EXISTENT/");
        snapshot =
                resourceManager.waitForResourceToBeInState(
                        snapshot, s -> FAILED.equals(s.getStatus().getState()));

        assertThat(snapshot.getStatus().getFailures()).isEqualTo(1);
        assertThat(snapshot.getStatus().getError()).contains("IO-problem detected");
    }

    private String assertManualSavepointNonce(boolean isLegacy) {
        resourceManager.editResource(
                flinkResource,
                d -> d.getSpec().getJob().setSavepointTriggerNonce(System.currentTimeMillis()));

        String path;
        if (isLegacy) {
            var resource =
                    resourceManager.waitForResourceToBeInState(
                            flinkResource,
                            d ->
                                    d.getStatus()
                                                    .getJobStatus()
                                                    .getSavepointInfo()
                                                    .getLastSavepoint()
                                            != null);
            path =
                    resource.getStatus()
                            .getJobStatus()
                            .getSavepointInfo()
                            .getLastSavepoint()
                            .getLocation();
        } else {
            var snapshot = waitForSnapshot("MANUAL");
            path = snapshot.getStatus().getPath();
            assertThat(resourceManager.getResource(flinkResource))
                    .isPresent()
                    .get()
                    .satisfies(
                            f ->
                                    assertThat(
                                                    f.getStatus()
                                                            .getJobStatus()
                                                            .getSavepointInfo()
                                                            .getLastSavepoint())
                                            .isNull());
        }

        log.info("Created manual savepoint using trigerNonce: {}", path);
        return path;
    }

    private void assertUpgradeSavepoint() {
        resourceManager.editResource(
                flinkResource, d -> d.getSpec().getJob().setState(JobState.SUSPENDED));
        var snapshotUpgrade = waitForSnapshot("UPGRADE");

        // Assert that deployment status is updated with latest savepoint
        flinkResource = resourceManager.getResource(flinkResource).orElseThrow();
        assertThat(flinkResource.getStatus().getJobStatus().getUpgradeSnapshotReference())
                .isEqualTo(FlinkStateSnapshotReference.fromResource(snapshotUpgrade));

        log.info("Restarting job...");
        resourceManager.editResource(
                flinkResource, d -> d.getSpec().getJob().setState(JobState.RUNNING));
        resourceManager.waitForResourceToBeInState(
                flinkResource,
                d -> JobState.RUNNING.name().equals(d.getStatus().getJobStatus().getState()));
    }

    private void assertManualSnapshot(FlinkStateSnapshot snapshot) {
        snapshot =
                resourceManager.waitForResourceToBeInState(
                        snapshot,
                        s -> {
                            var state = s.getStatus().getState();
                            log.info(
                                    "Waiting for snapshot {} to be complete, current: {}...",
                                    s.getMetadata().getName(),
                                    state.name());
                            return COMPLETED.equals(state);
                        });
        log.info(
                "Snapshot triggerId={}, path={}",
                snapshot.getStatus().getTriggerId(),
                snapshot.getStatus().getPath());

        String expectedPathPrefix;
        if (snapshot.getSpec().isSavepoint()) {
            if (snapshot.getSpec().getSavepoint().getPath() != null) {
                expectedPathPrefix = snapshot.getSpec().getSavepoint().getPath();
            } else {
                expectedPathPrefix = minioCluster.getFlinkConfiguration().get(SAVEPOINT_DIRECTORY);
            }
        } else {
            expectedPathPrefix = minioCluster.getFlinkConfiguration().get(CHECKPOINTS_DIRECTORY);
        }
        assertThat(snapshot.getStatus().getPath()).startsWith(expectedPathPrefix);

        log.info("Deleting snapshot...");
        resourceManager.deleteResource(snapshot);
        resourceManager.waitForResourceToBeInState(snapshot, Objects::isNull);
    }

    private FlinkStateSnapshot createCheckpointSnapshot() {
        var spec =
                FlinkStateSnapshotSpec.builder()
                        .jobReference(jobRef)
                        .checkpoint(new CheckpointSpec())
                        .build();

        var metadata = new ObjectMeta();
        metadata.setName("test-checkpoint-" + UUID.randomUUID().toString().substring(0, 8));

        var snapshot = new FlinkStateSnapshot();
        snapshot.setMetadata(metadata);
        snapshot.setSpec(spec);

        return resourceManager.createResource(snapshot);
    }

    private FlinkStateSnapshot createSavepointSnapshot(@Nullable String path) {
        var savepointSpec = new SavepointSpec();
        savepointSpec.setDisposeOnDelete(true);
        if (path != null) {
            savepointSpec.setPath(path);
        }

        var spec =
                FlinkStateSnapshotSpec.builder()
                        .backoffLimit(0)
                        .jobReference(jobRef)
                        .savepoint(savepointSpec)
                        .build();

        var metadata = new ObjectMeta();
        metadata.setName("test-savepoint-" + UUID.randomUUID().toString().substring(0, 8));

        var snapshot = new FlinkStateSnapshot();
        snapshot.setMetadata(metadata);
        snapshot.setSpec(spec);

        return resourceManager.createResource(snapshot);
    }

    private AbstractFlinkResource<?, ?> createFlinkDeployment(
            boolean disableSnapshotResources, boolean isSessionMode) {
        return createFlinkDeployment(
                disableSnapshotResources, isSessionMode, null, new Configuration());
    }

    private AbstractFlinkResource<?, ?> createFlinkDeployment(
            boolean disableSnapshotResources,
            boolean isSessionMode,
            @Nullable FlinkStateSnapshotReference snapshotRef) {
        return createFlinkDeployment(
                disableSnapshotResources, isSessionMode, snapshotRef, new Configuration());
    }

    private AbstractFlinkResource<?, ?> createFlinkDeployment(
            boolean disableSnapshotResources,
            boolean isSessionMode,
            @Nullable FlinkStateSnapshotReference snapshotRef,
            Configuration customConf) {
        var conf = DEFAULT_JOB_CONF.clone();
        conf.addAll(minioCluster.getFlinkConfiguration());
        conf.addAll(customConf);

        if (disableSnapshotResources) {
            conf.setString("kubernetes.operator.snapshot.resource.enabled", "false");
        }

        var jobSpec =
                JobSpec.builder()
                        .state(JobState.RUNNING)
                        .jarURI("local:///opt/flink/examples/streaming/StateMachineExample.jar")
                        .upgradeMode(UpgradeMode.SAVEPOINT)
                        .flinkStateSnapshotReference(snapshotRef)
                        .parallelism(1)
                        .build();

        var flinkDeployment =
                new FlinkDeploymentBuilder()
                        .withAppName(APP_NAME)
                        .withFlinkImage(FLINK_IMAGE)
                        .withKubernetesDeploymentMode(KubernetesDeploymentMode.NATIVE)
                        .withFlinkConfiguration(conf.toMap())
                        .build();

        if (!isSessionMode) {
            flinkDeployment.getSpec().setJob(jobSpec);
            return resourceManager.waitForAppToBeRunning(
                    resourceManager.createResource(flinkDeployment));
        }

        jobSpec.setJarURI(
                "https://repo1.maven.org/maven2/org/apache/flink/flink-examples-streaming/1.19.1/flink-examples-streaming-1.19.1.jar");
        jobSpec.setEntryClass(
                "org.apache.flink.streaming.examples.statemachine.StateMachineExample");

        resourceManager.createResource(flinkDeployment);
        var flinkSessionJob = new FlinkSessionJob();
        flinkSessionJob.setSpec(
                FlinkSessionJobSpec.builder()
                        .deploymentName(flinkDeployment.getMetadata().getName())
                        .flinkConfiguration(conf.toMap())
                        .job(jobSpec)
                        .build());
        flinkSessionJob.setMetadata(new ObjectMetaBuilder().withName(APP_NAME).build());

        return resourceManager.waitForAppToBeRunning(
                resourceManager.createResource(flinkSessionJob));
    }
}
