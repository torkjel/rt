package com.github.torkjel.rt.api;

import java.util.function.Supplier;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;

import com.github.torkjel.rt.api.config.Config;
import com.github.torkjel.rt.api.dispatcher.Dispatcher;
import com.github.torkjel.rt.api.dispatcher.WorkerClient;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Services {

    private static final Services INSTANCE = new Services();

    public static Services instance() {
        return INSTANCE;
    }

    private final AsyncHttpClient httpClient = new DefaultAsyncHttpClient();
    private ApiMain main;
    private Config config;
    private final Singleton<WorkerClient[]> clients = new Singleton<>();
    private final Singleton<Dispatcher> dispatcher = new Singleton<>();

    public Dispatcher getDispatcher() {
        return dispatcher.get(() -> new Dispatcher(config.getCluster(), httpClient));
    }

    private static class Singleton<T> {

        private T value;

        public synchronized T get(Supplier<T> s) {
            if (value == null)
                value = s.get();
            return value;
        }
    }
}
