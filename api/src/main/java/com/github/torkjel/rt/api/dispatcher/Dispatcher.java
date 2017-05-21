package com.github.torkjel.rt.api.dispatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.asynchttpclient.AsyncHttpClient;

import com.github.torkjel.rt.api.config.Cluster;
import com.github.torkjel.rt.api.config.ClusterChangedEvent;
import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;
import com.google.common.eventbus.Subscribe;

import lombok.extern.log4j.Log4j;

@Log4j
public class Dispatcher implements AutoCloseable {

    private volatile Cluster cluster;
    private final AsyncHttpClient httpClient;

    private final Map<String, WorkerClient> clients = new HashMap<>();

    public Dispatcher(Cluster cluster, AsyncHttpClient httpClient) {
        this.httpClient = httpClient;
        loadFromCluster(cluster);
    }

    public void submit(Event e) {
        Event anonymized = e.anonymized(cluster.getSliceNumber(e.getTimestamp()));
        getWorkerByHash(e.getTimestamp(), anonymized.getRoutingKey()).submit(anonymized);
    }

    public void retrieve(long timestamp, BiConsumer<Long, HourStats> callback) {
        List<WorkerClient> validWorkers = getActiveWorkers(timestamp);

        AtomicInteger count = new AtomicInteger(validWorkers.size());
        List<HourStats> stats = Collections.synchronizedList(new ArrayList<>());

        log.debug("Retrieving from " + count + " workers " + cluster.getSliceNumber(timestamp));

        BiConsumer<String, HourStats> workerCallback = (url, hs) -> {
            log.debug("Got results from worker " + url + ": " + hs);
            stats.add(hs);
            if (count.decrementAndGet() == 0) {
                HourStats aggregated = stats.stream().reduce(HourStats::combine).orElse(HourStats.empty());
                log.debug("Aggregated: " + aggregated);
                callback.accept(timestamp, aggregated);
            }
        };

        validWorkers.forEach(wc -> wc.retrieve(cluster.getSliceNumber(timestamp), workerCallback));
    }

    public void retrieveDetailed(long timestamp, Consumer<String> callback) {
        List<WorkerClient> validWorkers = getActiveWorkers(timestamp);

        AtomicInteger count = new AtomicInteger(validWorkers.size());
        Map<String, HourStats> stats = Collections.synchronizedMap(new HashMap<>());

        long slice = cluster.getSliceNumber(timestamp);

        log.debug("Retrieving from " + count + " workers");

        BiConsumer<String, HourStats> workerCallback = (url, hs) -> {
            log.debug("Got results from worker " + url + ": " + hs);
            stats.put(url, hs);
            if (count.decrementAndGet() == 0) {
                StringBuilder sb = new StringBuilder();
                stats.forEach((u, h) -> sb
                        .append("slice,").append(slice).append("\n")
                        .append("worker,").append(u).append("\n")
                        .append(h.toString()));
                callback.accept(sb.toString());
            }
        };

        validWorkers.forEach(wc -> wc.retrieve(cluster.getSliceNumber(timestamp), workerCallback));
    }

    public void retrieveAllSlices(long timestamp, Consumer<String> callback) {
        long start = 0;
        long end = cluster.getSliceNumber(timestamp);

        AtomicLong count = new AtomicLong(end - start + 1);
        Map<Long, HourStats> stats = Collections.synchronizedMap(new TreeMap<>());

        BiConsumer<Long, HourStats> sliceCallback = (sliceTimestamp, hs) -> {
            stats.put(sliceTimestamp, hs);
            if (count.decrementAndGet() <= 0) {
                StringBuilder sb = new StringBuilder();
                stats.forEach((u, h) -> sb
                        .append("slice,").append(cluster.getSliceNumber(u)).append("\n")
                        .append(h.toString()));
                callback.accept(sb.toString());
            }
        };

        if (end >= start)
            for (long n = start; n <= end; n++)
                retrieve(cluster.getStartOfFirstSlice() + n * cluster.getLengthOfSlice(), sliceCallback);
        else
            callback.accept("");
    }


    private List<WorkerClient> getActiveWorkers(long timestamp) {
        return cluster.getUrlsFor(timestamp)
                .stream()
                .map(url -> clients.get(url))
                .collect(Collectors.toList());
    }

    private WorkerClient getWorkerByHash(long timestamp, int hash) {
        return clients.get(cluster.getUrlFor(timestamp, hash));
    }

    @Override
    public void close() {
        blockUtilIdle();
        clients.values().forEach(WorkerClient::close);
    }

    public void blockUtilIdle() {
        while (clients.values().stream().map(WorkerClient::isIdle).anyMatch(idle -> !idle)) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                // continue loop
            }
        }
    }

    @Subscribe
    public void clusterChanged(ClusterChangedEvent e) {
        log.info("Got new cluster config");
        loadFromCluster(e.getCluster());
    }

    private void loadFromCluster(Cluster cluster) {
        this.cluster = cluster;
        for (String url : cluster.getWorkerNodes().values())
            if (!clients.containsKey(url))
                    clients.put(url, new WorkerClient(httpClient, url, cluster));
    }
}
