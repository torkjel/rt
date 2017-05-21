package com.github.torkjel.rt.api.dispatcher;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import org.asynchttpclient.exception.TooManyConnectionsException;
import org.asynchttpclient.exception.TooManyConnectionsPerHostException;

import com.github.torkjel.rt.api.config.Cluster;
import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;
import com.github.torkjel.rt.api.utils.Cache;

import lombok.extern.log4j.Log4j;

@Log4j
public class WorkerClient {

    private final String baseUrl;
    private final AsyncHttpClient asyncHttpClient;

    private final AtomicInteger maxQueue = new AtomicInteger(0);
    private final AtomicInteger queuedQueries = new AtomicInteger(0);

    private final Cache<Long, HourStats> statsCache = new Cache<>(1000);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public WorkerClient(AsyncHttpClient client, String url, Cluster cluster) {
        this.baseUrl = url;
        this.asyncHttpClient = client;
    }

    public void submit(Event e) {

        queuedQueries.incrementAndGet();

        int queueSize = queuedQueries.get();
        if (queueSize > maxQueue.get()) {
            maxQueue.set(queueSize);
            log.warn("Max queue: " + queueSize + ", url: " + baseUrl);
        }

        internalSubmit(e);
    }

    private void internalSubmit(Event e) {
        String url = baseUrl + "?" + e.toUrlQueryPart();
        log.info(this + "POSTing " + url);

        asyncHttpClient
            .preparePost(url)
            .execute(
                    new AsyncCompletionHandler<Response>() {

                        @Override
                        public Response onCompleted(Response response) throws Exception{
                            log.info("POSTed " + url + " : " + response.getStatusCode() + "\n" +
                                    response.getResponseBody());
                            if (response.getStatusCode() == 200) {
                                statsCache.update(e.getSlice(), HourStats.parse(response.getResponseBody()));
                            }
                            queuedQueries.decrementAndGet();
                            return response;
                        }

                        @Override
                        public void onThrowable(Throwable t){
                            handleInternalError(t, url, () -> internalSubmit(e));
                        }

                    });
    }

    public void retrieve(long slice, BiConsumer<String, HourStats> callback) {

        Optional<HourStats> cached = statsCache.get(slice);
        if (cached.isPresent()) {
            callback.accept(baseUrl, cached.get());
        } else {
            queuedQueries.incrementAndGet();
            internalRetrieve(slice, callback);
        }
    }

    private void internalRetrieve(long slice, BiConsumer<String, HourStats> callback) {

        String url = baseUrl + "?slice=" + slice;

        log.info("GETing " + url);

        asyncHttpClient
            .prepareGet(url)
            .execute(
                    new AsyncCompletionHandler<Response>() {

                        @Override
                        public Response onCompleted(Response response) throws Exception{
                            log.info("GOT " + url + " : " + response.getStatusCode() + " \n " +
                                    response.getResponseBody());
                            HourStats stats = HourStats.parse(response.getResponseBody());
                            statsCache.update(slice, stats);
                            callback.accept(baseUrl, stats);
                            queuedQueries.decrementAndGet();
                            return response;
                        }

                        @Override
                        public void onThrowable(Throwable t){
                            handleInternalError(t, url, () -> internalRetrieve(slice, callback));
                        }
                    });
    }

    private void handleInternalError(Throwable t, String url, Runnable retry) {
        if (t instanceof TooManyConnectionsException
                || t instanceof TooManyConnectionsPerHostException) {
            log.warn("Too many connections. Rescheduling. " +
                    "Queue = " + queuedQueries.get() + ", " +
                    "Url = " + baseUrl);
            executor.schedule(retry,
                    queuedQueries.get() * 3,
                    TimeUnit.MILLISECONDS);
        } else {
            // TODO: metrics should report errors.
            log.error("Request " + url + " failed", t);
            t.printStackTrace();
        }
    }

    public boolean isIdle() {
        return queuedQueries.get() == 0;
    }

    public void close() {
        executor.shutdown();
    }

}
