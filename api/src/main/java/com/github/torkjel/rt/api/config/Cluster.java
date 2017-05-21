package com.github.torkjel.rt.api.config;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class Cluster {

    private final Map<String, String> apiNodes;
    private final Map<String, String> workerNodes;
    private final long lengthOfSlice;
    private final long startOfFirstSlice;
    private final List<Routing> routing;

    @JsonCreator
    public Cluster(
            @JsonProperty("api-nodes") Map<String, String> apiNodes,
            @JsonProperty("worker-nodes") Map<String, String> workerNodes,
            @JsonProperty("length-of-time-slice") int lengthOfSlice,
            @JsonProperty("first-slice") String firstSlice,
            @JsonProperty("routing") List<Routing> routing) {
        this.apiNodes = apiNodes;
        this.workerNodes = workerNodes;
        this.lengthOfSlice = lengthOfSlice;
        this.startOfFirstSlice = "system".equals(firstSlice)
                ? System.currentTimeMillis() / 1000
                : LocalDateTime.parse(firstSlice).toEpochSecond(ZoneOffset.UTC);
        this.routing = routing;
    }

    public static Cluster parseResource(String resource) {
        try {
            return new ObjectMapper().readValue(
                    new InputStreamReader(Config.class.getResourceAsStream(resource), "UTF-8"),
                    Cluster.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse config", e);
        }
    }

    public static Cluster loadDefault() {
        return parseResource("/cluster.json");
    }

    public String getUrlFor(long timestamp, int routingKey) {

        Routing routing = findRouting(timestamp);
        String node = routing.getNode(routingKey);
        return workerNodes.get(node);
    }

    public List<String> getUrlsFor(long timestamp) {
        Routing routing = findRouting(timestamp);
        return routing.getNodes()
                .stream()
                .map(node -> workerNodes.get(node))
                .collect(Collectors.toList());
    }

    public long getSliceNumber(long timestamp) {
        return (timestamp - startOfFirstSlice) / lengthOfSlice;
    }

    private Routing findRouting(long timestamp) {
        // TODO: could cache this. cache key = (timestamp - startOfFirstSlice) / lengthOfSlice * lengthOfSlice
        Routing match = null;
        long startOfMatchingSlice = Long.MAX_VALUE;
        for (Routing r : routing) {
            long startOfSlice = r.startOfSlice(startOfFirstSlice, lengthOfSlice);
            if (timestamp >= startOfSlice && (match == null || startOfSlice > startOfMatchingSlice)) {
                startOfMatchingSlice = startOfSlice;
                match = r;
            }
        }
        return match;
    }

}
