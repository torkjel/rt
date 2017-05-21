package com.github.torkjel.rt.api.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.github.torkjel.rt.api.Services;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;

@Getter
@ToString
@Log4j
public class Config implements AutoCloseable {

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);

    private final String nodeId;
    private volatile Cluster cluster;
    private final String configFile;

    @Builder
    public Config(String nodeId, String configFile) {
        this.nodeId = nodeId;
        if (configFile != null) {
            this.cluster = Cluster.loadFromResource(configFile);
            this.configFile = configFile;
            executor.scheduleAtFixedRate(this::reloadUpdatedCluster, 60, 30, TimeUnit.SECONDS);
        } else {
            this.cluster = Cluster.loadDefault();;
            this.configFile = null;
        }
    }

    public void reloadUpdatedCluster() {
        log.info("Reloading config from " + configFile);
        Cluster newCluster = Cluster.loadFromResource(configFile);
        if (newCluster.hasChanged(cluster)) {
            log.warn("Cluster changed! Updating.");
            cluster = cluster.update(newCluster);
            Services.instance().getEventBus().post(new ClusterChangedEvent(cluster));
        }
    }

    public int getLocalPort() {
        String url = cluster.getApiNodes().get(nodeId);
        try {
            return new URI(url).getPort();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to parse url: " + url, e);
        }
    }

    public List<String> getWorkers() {
        return new ArrayList<>(cluster.getWorkerNodes().values());
    }

    public static Config parse(String[] args) {
        ConfigBuilder builder = Config.builder();

        if (args.length == 0)
            throw new IllegalArgumentException("Need node number argument");

        if (args.length >= 1)
            builder.nodeId(args[0]);

        if (args.length >= 2)
            builder.configFile(args[1]);

        return builder.build();
    }

    public void close() {
        executor.shutdownNow();
    }
}
