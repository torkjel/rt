package com.github.torkjel.rt.api.dispatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.asynchttpclient.AsyncHttpClient;

import com.github.torkjel.rt.api.config.Cluster;
import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;

import lombok.extern.log4j.Log4j;

@Log4j
public class Dispatcher implements AutoCloseable {

    private final Cluster cluster;
    private final AsyncHttpClient httpClient;

    private final Map<String, WorkerClient> clients = new HashMap<>();

    public Dispatcher(Cluster cluster, AsyncHttpClient httpClient) {
        this.cluster = cluster;
        this.httpClient = httpClient;
    }

    public void submit(Event e) {
        getWorkerByHash(e.getUser().charAt(0)).submit(e);
    }

    public void retrieve(long timestamp, Consumer<HourStats> callback) {

        log.info("Retrieving");

        List<WorkerClient> validWorkers = getValidWorkers();

        AtomicInteger count = new AtomicInteger(validWorkers.size());
        List<HourStats> stats = Collections.synchronizedList(new ArrayList<>());

        log.info("Retrieving" + count);

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

        validWorkers.forEach(wc -> wc.retrieve(timestamp, workerCallback));
    }

    private List<WorkerClient> getValidWorkers() {
        return IntStream.range(0, cluster.getRouting().size())
                .mapToObj(this::getWorkerByIndex)
                .collect(Collectors.toList());
    }

    private WorkerClient getWorkerByHash(int hash) {
        return getWorkerByIndex(hash % cluster.getRouting().size());
    }

    private WorkerClient getWorkerByIndex(int index) {
        String url = cluster.getUrlFor(index);
        WorkerClient client = clients.get(url);
        if (client == null)
            clients.put(url, client = new WorkerClient(httpClient, url));
        return client;
    }

    @Override
    public void close() throws Exception {
        blockUtilIdle();
        clients.values().forEach(WorkerClient::close);
    }

    public void blockUtilIdle() throws Exception {
        while (clients.values().stream().map(WorkerClient::isIdle).anyMatch(idle -> !idle))
            Thread.sleep(10);
    }

}
