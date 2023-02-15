package org.apache.flink.operator.testing.app;

import io.fabric8.kubernetes.api.model.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.*;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.operator.testing.Constants.KUBERNETES_CONTAINER_FLINK_NAME;

@With
@AllArgsConstructor
@NoArgsConstructor
public class FlinkDeploymentBuilder {
    private static final Map<String, String> DEFAULT_FLINK_CONFIG = Map.of();
    private static final Map<String, String> DEBUG_LOG = Map.of("log4j-console.properties", "rootLogger.level = DEBUG\n" +
            "rootLogger.appenderRef.file.ref = LogFile\n" +
            "rootLogger.appenderRef.console.ref = LogConsole\n" +
            "appender.file.name = LogFile\n" +
            "appender.file.type = File\n" +
            "appender.file.append = false\n" +
            "appender.file.fileName = ${sys:log.file}\n" +
            "appender.file.layout.type = PatternLayout\n" +
            "appender.file.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n\n" +
            "appender.console.name = LogConsole\n" +
            "appender.console.type = CONSOLE\n" +
            "appender.console.layout.type = PatternLayout\n" +
            "appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n\n" +
            "logger.akka.name = akka\n" +
            "logger.akka.level = INFO\n" +
            "logger.kafka.name= org.apache.kafka\n" +
            "logger.kafka.level = INFO\n" +
            "logger.hadoop.name = org.apache.hadoop\n" +
            "logger.hadoop.level = INFO\n" +
            "logger.zookeeper.name = org.apache.zookeeper\n" +
            "logger.zookeeper.level = INFO\n" +
            "logger.netty.name = org.apache.flink.shaded.akka.org.jboss.netty.channel.DefaultChannelPipeline\n" +
            "logger.netty.level = OFF");
    private String appName;
    private Long restartNonce;
    private String flinkImage;
    private KubernetesDeploymentMode kubernetesDeploymentMode;
    private Map<String, String> flinkConfiguration = Map.of();
    private JobSpec jobSpec = null;
    private IngressSpec ingressSpec = null;
    private Boolean sessionMode = false;
    private Boolean debugLog = false;
    private String flinkServiceAccount = "flink";
    private Integer jobManagerReplicas = 1;
    private Double jobManagerCpu = 0.5D;
    private String jobManagerMemory = "1g";
    private Double taskManagerCpu = 0.5D;
    private Integer taskManagerReplicas = null;
    private String taskManagerMemory = "1g";

    public FlinkDeployment build() {
        var finalFlinkConfiguration = new HashMap<>(DEFAULT_FLINK_CONFIG);
        finalFlinkConfiguration.putAll(flinkConfiguration);

        var flinkDeployment = new FlinkDeployment();
        flinkDeployment.setMetadata(new ObjectMetaBuilder()
                .withName(appName)
                .build());

        var flinkDeploymentSpecBuilder = FlinkDeploymentSpec.builder()
                .restartNonce(restartNonce)
                .image(flinkImage)
                .imagePullPolicy("Always")
                .serviceAccount(flinkServiceAccount)
                .flinkVersion(FlinkVersion.v1_18)
                .jobManager(createJobManagerSpec())
                .taskManager(createTaskManagerSpec())
                .flinkConfiguration(finalFlinkConfiguration)
                .ingress(ingressSpec);

        if (debugLog) {
            flinkDeploymentSpecBuilder = flinkDeploymentSpecBuilder.flinkConfiguration(DEBUG_LOG);
        }
        if (!sessionMode) {
            flinkDeploymentSpecBuilder = flinkDeploymentSpecBuilder
                    .mode(kubernetesDeploymentMode)
                    .job(jobSpec);
        }

        flinkDeployment.setSpec(flinkDeploymentSpecBuilder.build());
        return flinkDeployment;
    }

    private Pod createPodTemplate() {
        var podSpecBuilder = new PodSpecBuilder()
                .withVolumes(new VolumeBuilder()
                        .withName("flink-volume")
                        .withPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource(appName, false))
                        .build());
        var flinkContainerBuilder = new ContainerBuilder()
                .withName(KUBERNETES_CONTAINER_FLINK_NAME)
                .withVolumeMounts(new VolumeMountBuilder()
                        .withMountPath("/flink-data")
                        .withName("flink-volume")
                        .build()
                );

        podSpecBuilder.addToContainers(flinkContainerBuilder.build());
        return new PodBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("pod-template").build())
                .withSpec(podSpecBuilder.build())
                .build();
    }

    private JobManagerSpec createJobManagerSpec() {
        return JobManagerSpec.builder()
                .podTemplate(createPodTemplate())
                .resource(new Resource(jobManagerCpu, jobManagerMemory, "2G"))
                .replicas(jobManagerReplicas)
                .build();
    }

    private TaskManagerSpec createTaskManagerSpec() {
        return TaskManagerSpec.builder()
                .podTemplate(createPodTemplate())
                .resource(new Resource(taskManagerCpu, taskManagerMemory, "2G"))
                .replicas(taskManagerReplicas)
                .build();
    }
}

