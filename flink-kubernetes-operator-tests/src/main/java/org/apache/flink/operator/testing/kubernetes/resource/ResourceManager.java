package org.apache.flink.operator.testing.kubernetes.resource;

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;

import io.fabric8.kubernetes.api.builder.Visitor;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Gettable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class ResourceManager implements AutoCloseable {
    private static final Map<String, String> LABELS =
            Map.of("created-by", "flink-kubernetes-operator-testing");

    private final KubernetesClient kubernetesClient;
    private final Stack<HasMetadata> createdResources = new Stack<>();

    public ResourceManager(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    @Override
    public void close() {
        for (var resourceName : createdResources) {
            var resource = kubernetesClient.resource(resourceName).get();
            if (resource != null) {
                log.info(
                        "Deleting {} {}/{}",
                        resource.getKind(),
                        resourceName.getMetadata().getNamespace(),
                        resource.getMetadata().getName());
                kubernetesClient.resource(resource).withTimeout(300, TimeUnit.SECONDS).delete();
                waitForResourceToBeInState(resource, Objects::isNull);
            }
        }
    }

    public <T extends HasMetadata> T createResource(T resource) {
        var appName = resource.getMetadata().getName();
        log.info("Submitting {}: {}", resource.getKind(), appName);
        var result = kubernetesClient.resource(resource).create();
        createdResources.add(result);
        return result;
    }

    public void deleteResource(HasMetadata resource) {
        kubernetesClient.resource(resource).delete();
        createdResources.removeIf(
                m ->
                        m.getMetadata().getName().equals(resource.getMetadata().getName())
                                && m.getMetadata()
                                        .getNamespace()
                                        .equals(resource.getMetadata().getNamespace()));
    }

    public <T extends HasMetadata> T editResource(T resource, Visitor<T> visitor) {
        // First get resource to make sure we edit the latest version
        var resourceName =
                String.format(
                        "%s/%s",
                        resource.getMetadata().getNamespace(), resource.getMetadata().getName());
        resource =
                getResource(resource)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                String.format(
                                                        "Resource %s does not exist",
                                                        resourceName)));

        return kubernetesClient
                .resource(resource)
                .edit(
                        r -> {
                            visitor.visit(r);
                            return r;
                        });
    }

    public <T extends HasMetadata> List<T> getResources(Class<T> clazz) {
        return kubernetesClient.resources(clazz).list().getItems();
    }

    public <T extends HasMetadata> Optional<T> getResource(T resource) {
        return Optional.ofNullable(kubernetesClient.resource(resource).get());
    }

    public <T extends HasMetadata> Optional<T> getResource(Class<T> clazz, String name) {
        return Optional.ofNullable(kubernetesClient.resources(clazz).withName(name).get());
    }

    public List<Event> getResourceEvents(HasMetadata resource) {
        var objectRef =
                new ObjectReferenceBuilder()
                        .withKind(resource.getKind())
                        .withUid(resource.getMetadata().getUid())
                        .withNamespace(resource.getMetadata().getNamespace())
                        .withName(resource.getMetadata().getName())
                        .build();
        return kubernetesClient
                .v1()
                .events()
                .withInvolvedObject(objectRef)
                .resources()
                .map(Gettable::get)
                .collect(Collectors.toList());
    }

    public String getFlinkJobManagerPodLogs(String appName) {
        var labels = Map.of("component", "jobmanager", "app", appName);
        return kubernetesClient
                .pods()
                .withLabels(labels)
                .resources()
                .findFirst()
                .orElseThrow()
                .getLog(true);
    }

    public <T extends HasMetadata> T waitForResourceToBeInState(
            T resource, Predicate<T> predicate) {
        return kubernetesClient
                .resource(resource)
                .waitUntilCondition(
                        r -> {
                            try {
                                return predicate.test(r);
                            } catch (Throwable t) {
                                log.error(
                                        "Error while testing {}",
                                        resource.getMetadata().getName(),
                                        t);
                                return false;
                            }
                        },
                        300,
                        TimeUnit.SECONDS);
    }

    public <T extends AbstractFlinkResource<?, ?>> T waitForAppToBeRunning(T resource) {
        var name = resource.getMetadata().getName();
        log.info("Waiting for '{}' reconciliation to be done", name);
        resource =
                waitForResourceToBeInState(
                        resource,
                        r ->
                                ReconciliationState.DEPLOYED.equals(
                                        r.getStatus().getReconciliationStatus().getState()));

        log.info("Waiting for '{}' resource to be RUNNING", name);
        resource =
                waitForResourceToBeInState(
                        resource,
                        s ->
                                Set.of("RUNNING", "FAILING", "FAILED", "FINISHED")
                                        .contains(s.getStatus().getJobStatus().getState()));
        var jobState = resource.getStatus().getJobStatus().getState();

        if (!"RUNNING".equals(jobState) && !"FINISHED".equals(jobState)) {
            throw new RuntimeException(
                    String.format("Job '%s' failed to start, last state: %s", name, jobState));
        }

        log.info("Job '{}' state: {}", name, jobState);
        return resource;
    }
}
