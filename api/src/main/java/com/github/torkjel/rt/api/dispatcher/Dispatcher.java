package com.github.torkjel.rt.api.dispatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.asynchttpclient.AsyncHttpClient;

import com.github.torkjel.rt.api.config.Cluster;
import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;

import lombok.extern.log4j.Log4j;

@Log4j
public class Dispatcher implements AutoCloseable {

    private final Cluster cluster;

    private final Map<String, WorkerClient> clients = new HashMap<>();

    public Dispatcher(Cluster cluster, AsyncHttpClient httpClient) {
        this.cluster = cluster;
        for (String url : cluster.getWorkerNodes().values())
            clients.put(url, new WorkerClient(httpClient, url, cluster));
    }

    public void submit(Event e) {
        Event anonymized = e.anonymized(cluster.getSliceNumber(e.getTimestamp()));
        getWorkerByHash(e.getTimestamp(), anonymized.getUser().charAt(0)).submit(anonymized);
    }

    public void retrieve(long timestamp, Consumer<HourStats> callback) {

        log.info("Retrieving");

        List<WorkerClient> validWorkers = getActiveWorkers(timestamp);

        AtomicInteger count = new AtomicInteger(validWorkers.size());
        List<HourStats> stats = Collections.synchronizedList(new ArrayList<>());

        log.info("Retrieving from " + count + " workers");

        Consumer<HourStats> workerCallback = (hs) -> {
            log.info("Got results from worker: " + hs);
            stats.add(hs);
            if (count.decrementAndGet() == 0) {
                log.info("Got results from all workers");
                HourStats aggregated = stats.stream().reduce(HourStats::combine).orElse(HourStats.empty());
                log.info("Aggregated: " + aggregated);
                callback.accept(aggregated);
            }
        };

        validWorkers.forEach(wc -> wc.retrieve(cluster.getSliceNumber(timestamp), workerCallback));
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

    public String toString() {
        return clients.toString();
    }


}
