package com.github.torkjel.rt.api.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@Builder
@ToString
public class Config {

    private final String nodeId;
    private final Cluster cluster;

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
            builder.cluster(Cluster.parseResource(args[1]));
        else
            builder.cluster(Cluster.loadDefault());

        return builder.build();
    }
}
