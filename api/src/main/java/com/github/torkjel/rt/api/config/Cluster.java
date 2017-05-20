package com.github.torkjel.rt.api.config;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Getter;

@Getter
public class Cluster {

    private final Map<String, String> apiNodes;
    private final Map<String, String> workerNodes;
    private final List<String> routing;

    @JsonCreator
    public Cluster(
            @JsonProperty("api-nodes") Map<String, String> apiNodes,
            @JsonProperty("worker-nodes") Map<String, String> workerNodes,
            @JsonProperty("routing") List<String> routing) {
        this.apiNodes = apiNodes;
        this.workerNodes = workerNodes;
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

    public String getUrlFor(int routingIndex) {
        return workerNodes.get(routing.get(routingIndex));
    }

}
