package com.github.torkjel.rt.api.dispatcher;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.asynchttpclient.*;
import org.asynchttpclient.exception.TooManyConnectionsException;
import org.asynchttpclient.exception.TooManyConnectionsPerHostException;

import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;
import com.github.torkjel.rt.api.utils.Cache;
import com.github.torkjel.rt.api.utils.TimeUtils;

import lombok.extern.log4j.Log4j;

@Log4j
public class WorkerClient {

    private final String baseUrl;
    private final AsyncHttpClient asyncHttpClient;

    private final AtomicInteger maxQueue = new AtomicInteger(0);
    private final AtomicInteger queuedQueries = new AtomicInteger(0);

    private final Cache<Long, HourStats> statsCache = new Cache<>(1000);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public WorkerClient(AsyncHttpClient client, String url) {
        this.baseUrl = url;
        this.asyncHttpClient = client;
    }

    public void submit(Event e) {

        queuedQueries.incrementAndGet();

        int queueSize = queuedQueries.get();
        if (queueSize > maxQueue.get()) {
            maxQueue.set(queueSize);
            log.warn("Max queue: " + queueSize + " " + baseUrl);
        }

        String url = baseUrl + "?" + e.toUrlQueryPart();
        log.info(this + "POSTing " + url);

        asyncHttpClient
            .preparePost(url)
            .execute(
                    new AsyncCompletionHandler<Response>(){
                        @Override
                        public Response onCompleted(Response response) throws Exception{
                            log.info("POSTed " + url + " : " + response.getStatusCode() + "\n" + response.getResponseBody());
                            if (response.getStatusCode() == 200)
                                statsCache.update(e.getHourStart(), HourStats.parse(response.getResponseBody()));
                            queuedQueries.decrementAndGet();
                            return response;
                        }
                        @Override
                        public void onThrowable(Throwable t){
                            if (t instanceof TooManyConnectionsException
                                    || t instanceof TooManyConnectionsPerHostException) {
                                log.warn("Too many connections. Rescheduling. Queue = " + queuedQueries.get() + ", url = " + baseUrl);
                                executor.schedule(() -> WorkerClient.this.submit(e), queuedQueries.get() * 3, TimeUnit.MILLISECONDS);
                            } else {
                                // TODO: metrics should report errors.
                                log.error("Request " + url + " failed", t);
                                t.printStackTrace();
                            }
                            queuedQueries.decrementAndGet();
                        }
                    });
    }

    public boolean isIdle() {
        return queuedQueries.get() == 0;
    }

    public void close() {
        executor.shutdown();
    }

    public void retrieve(long timestamp, Consumer<HourStats> callback) {

        String url = baseUrl + "?timestamp=" + timestamp;

        log.info("GETing " + url);

        Optional<HourStats> cached = statsCache.get(TimeUtils.startOfHour(timestamp));
        if (cached.isPresent()) {
            callback.accept(cached.get());
        } else {
            asyncHttpClient
                .prepareGet(url)
                .execute(
                        new AsyncCompletionHandler<Response>(){
                            @Override
                            public Response onCompleted(Response response) throws Exception{
                                log.info("GOT " + url + " : " + response.getStatusCode() + " \n " + response.getResponseBody());
                                callback.accept(HourStats.parse(response.getResponseBody()));
                                return response;
                            }
                            @Override
                            public void onThrowable(Throwable t){
                                // TODO: metrics should report errors.
                                log.error("Request " + url + " failed", t);
                            }
                        });
        }
    }

}
