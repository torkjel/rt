package com.github.torkjel.rt.api;

import java.util.function.Supplier;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import com.github.torkjel.rt.api.config.Config;
import com.github.torkjel.rt.api.dispatcher.Dispatcher;
import com.google.common.eventbus.EventBus;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Services implements AutoCloseable {

    private static final Services INSTANCE = new Services();

    public static Services instance() {
        return INSTANCE;
    }

    private ApiMain main;
    private Config config;

    private final EventBus eventBus = new EventBus();

    private final Singleton<AsyncHttpClient> httpClient = new Singleton<>();
    private final Singleton<Dispatcher> dispatcher = new Singleton<>();

    public AsyncHttpClient getHttpClient() {
        return httpClient.get(
                () -> new DefaultAsyncHttpClient(
                        new DefaultAsyncHttpClientConfig.Builder()
                        .setMaxConnectionsPerHost(600)
                        .setMaxConnections(600)
                        .build()));
    }

    public Dispatcher getDispatcher() {
        return dispatcher.get(() -> {
            Dispatcher d = new Dispatcher(config.getCluster(), getHttpClient());
            eventBus.register(d);
            return d;
        });
    }

    public void close() {
        eventBus.unregister(dispatcher);
        config.close();
        main.stop();
        dispatcher.get().close();
        try {
            httpClient.get().close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to close http client", e);
        }
        dispatcher.reset();
        httpClient.reset();
    }

    public void blockUtilIdle() {
        dispatcher.get().blockUtilIdle();
    }

    private static class Singleton<T> {

        private T value;

        public synchronized T get(Supplier<T> s) {
            if (value == null)
                value = s.get();
            return value;
        }

        public T get() {
            return value;
        }

        public void reset() {
            value = null;
        }
    }
}
