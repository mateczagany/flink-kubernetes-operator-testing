package org.apache.flink.operator.testing.kubernetes.resource;

import org.apache.flink.configuration.Configuration;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpecBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import lombok.Getter;

import java.util.List;
import java.util.UUID;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.configuration.HighAvailabilityOptions.HA_STORAGE_PATH;

public class MinioCluster extends AbstractKubernetesService {

    private static final String IMAGE = "minio/minio:RELEASE.2024-07-16T23-46-41Z";
    private static final String IMAGE_BUSYBOX = "busybox:1.36.1";
    public static final String BUCKET_NAME = "test";

    @Getter private String accessKey;
    @Getter private String secretKey;
    private int consolePort;
    private int apiPort;

    public MinioCluster(ResourceManager resourceManager) {
        super(resourceManager);
    }

    public Configuration getFlinkConfiguration() {
        var conf = new Configuration();
        conf.set(SAVEPOINT_DIRECTORY, String.format("s3://%s/savepoints/", BUCKET_NAME));
        conf.set(CHECKPOINTS_DIRECTORY, String.format("s3://%s/checkpoints/", BUCKET_NAME));
        conf.set(HA_STORAGE_PATH, String.format("s3://%s/ha", BUCKET_NAME));
        conf.setString("s3.endpoint", String.format("http://%s:%s", getHostname(), apiPort));
        conf.setString("s3.access-key", accessKey);
        conf.setString("s3.secret-key", secretKey);
        conf.setString("s3.path.style.access", "true");
        return conf;
    }

    @Override
    protected void initialize() {
        accessKey = UUID.randomUUID().toString();
        secretKey = UUID.randomUUID().toString();
        consolePort = findRandomOpenPort();
        apiPort = findRandomOpenPort();
    }

    @Override
    protected String getNamePrefix() {
        return "minio";
    }

    @Override
    protected List<HasMetadata> createResources() {
        return List.of(createPod());
    }

    @Override
    protected List<Service> createPortForwardedServices() {
        return List.of(createService());
    }

    private Pod createPod() {
        var metadata =
                new ObjectMetaBuilder().withName(name).withLabels(getSelectorLabels()).build();

        var envVars =
                List.of(
                        new EnvVar("MINIO_ROOT_USER", accessKey, null),
                        new EnvVar("MINIO_ROOT_PASSWORD", secretKey, null));

        var container =
                new ContainerBuilder()
                        .withName("minio")
                        .withImage(IMAGE)
                        .addToArgs("server", "/buckets")
                        .addToArgs("--address", String.format(":%d", apiPort))
                        .addToArgs("--console-address", String.format(":%d", consolePort))
                        .addToVolumeMounts(
                                new VolumeMountBuilder()
                                        .withName("bucket")
                                        .withMountPath("/buckets")
                                        .build())
                        .addToPorts(
                                new ContainerPortBuilder()
                                        .withName("api")
                                        .withContainerPort(apiPort)
                                        .build())
                        .addToPorts(
                                new ContainerPortBuilder()
                                        .withName("console")
                                        .withContainerPort(consolePort)
                                        .build())
                        .withEnv(envVars)
                        .build();

        var initContainer =
                new ContainerBuilder()
                        .withName("minio-init")
                        .withImage(IMAGE_BUSYBOX)
                        .addToCommand(
                                "sh", "-c", String.format("mkdir -p /buckets/%s", BUCKET_NAME))
                        .addToVolumeMounts(
                                new VolumeMountBuilder()
                                        .withName("bucket")
                                        .withMountPath("/buckets")
                                        .build())
                        .build();

        var spec =
                new PodSpecBuilder()
                        .withVolumes(
                                new VolumeBuilder()
                                        .withName("bucket")
                                        .withNewEmptyDir()
                                        .withSizeLimit(Quantity.parse("500Mi"))
                                        .endEmptyDir()
                                        .build())
                        .withInitContainers(initContainer)
                        .withContainers(container)
                        .build();
        var pod = new PodBuilder().withMetadata(metadata).withSpec(spec).build();

        return resourceManager.createResource(pod);
    }

    private Service createService() {
        var metadata = new ObjectMetaBuilder().withName(name).build();
        var spec =
                new ServiceSpecBuilder()
                        .withSelector(getSelectorLabels())
                        .addToPorts(
                                new ServicePortBuilder()
                                        .withProtocol("TCP")
                                        .withName("console")
                                        .withPort(consolePort)
                                        .build())
                        .addToPorts(
                                new ServicePortBuilder()
                                        .withProtocol("TCP")
                                        .withName("api")
                                        .withPort(apiPort)
                                        .build())
                        .build();

        var service = new ServiceBuilder().withMetadata(metadata).withSpec(spec).build();

        return resourceManager.createResource(service);
    }
}
