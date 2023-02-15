package org.apache.flink.operator.testing.kubernetes.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.LocalPortForward;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.burningwave.tools.net.DefaultHostResolver;
import org.burningwave.tools.net.HostResolutionRequestInterceptor;
import org.burningwave.tools.net.MappedHostResolver;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public abstract class AbstractKubernetesService implements AutoCloseable {
    protected final String name;
    protected final ResourceManager resourceManager;
    protected final List<HasMetadata> createdResources = new ArrayList<>();
    protected final List<LocalPortForward> portForwards = new ArrayList<>();

    @SneakyThrows
    protected static int findRandomOpenPort() {
        try (ServerSocket socket = new ServerSocket(0); ) {
            return socket.getLocalPort();
        }
    }

    public AbstractKubernetesService(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
        this.name = getNamePrefix() + "-" + UUID.randomUUID().toString().substring(0, 6);

        initialize();
        createdResources.addAll(createResources());
        var services = createPortForwardedServices();
        createdResources.addAll(services);

        var hostAliases = Map.of(getHostname(), "127.0.0.1");
        HostResolutionRequestInterceptor.INSTANCE.install(
                new MappedHostResolver(hostAliases), DefaultHostResolver.INSTANCE);

        for (var service : services) {
            for (var port : service.getSpec().getPorts()) {
                var portForward =
                        resourceManager
                                .getKubernetesClient()
                                .services()
                                .withName(name)
                                .portForward(port.getPort(), port.getPort());
                portForwards.add(portForward);
                log.info(
                        "Service {} portforwarded: {}:{}",
                        service.getMetadata().getName(),
                        getHostname(),
                        port.getPort());
            }
        }
    }

    @SneakyThrows
    @Override
    public void close() {
        for (var portForward : portForwards) {
            portForward.close();
        }
        createdResources.forEach(resourceManager::deleteResource);
    }

    public String getHostname() {
        return String.format(
                "%s.%s.svc.cluster.local",
                name, resourceManager.getKubernetesClient().getConfiguration().getNamespace());
    }

    protected Map<String, String> getSelectorLabels() {
        return Map.of("app.kubernetes.io/name", name);
    }

    protected abstract void initialize();

    protected abstract List<HasMetadata> createResources();

    protected abstract List<Service> createPortForwardedServices();

    protected abstract String getNamePrefix();
}
