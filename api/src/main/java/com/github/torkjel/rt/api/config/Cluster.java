package com.github.torkjel.rt.api.config;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.log4j.Log4j;

@Getter
@ToString
@EqualsAndHashCode
@Log4j
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
            @JsonProperty("length-of-time-slice") long lengthOfSlice,
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

    public static Cluster loadFromResource(String resource) {
        File f = new File(resource);
        Reader r = null;
        try {
            if (f.exists() && f.isFile()) {
                r = new FileReader(f);
                log.info("Loading cluster config from file: " + f.getAbsolutePath());
            } else {
                InputStream is = Config.class.getResourceAsStream(resource);
                if (is != null) {
                    r = new InputStreamReader(is,  "UTF-8");
                    log.info("Loading cluster config from classpath : " + resource);
                }
            }
            if (r == null)
                throw new RuntimeException("File not found: " + resource);

            return new ObjectMapper().readValue(r, Cluster.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config " + resource, e);
        } finally {
            if (r != null)
                try {
                    r.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public static Cluster loadDefault() {
        return loadFromResource("/cluster.json");
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
        if (match == null) {
            log.debug("No routing for timestamp: " + timestamp + ". Returning routing for first slice");
            return findRouting(startOfFirstSlice);
        }

        return match;
    }

    public boolean hasChanged(Cluster that) {
        return !(that.getApiNodes().equals(getApiNodes())
                && that.getWorkerNodes().equals(getWorkerNodes())
                && that.getRouting().equals(getRouting()));
    }

    public Cluster update(Cluster that) {
        return new Cluster(
                that.getApiNodes(),
                that.getWorkerNodes(),
                getLengthOfSlice(),
                LocalDateTime.ofEpochSecond(getStartOfFirstSlice(), 0,  ZoneOffset.UTC).toString(),
                that.getRouting());
    }

}
