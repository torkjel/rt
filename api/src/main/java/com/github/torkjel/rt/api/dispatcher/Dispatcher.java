package com.github.torkjel.rt.api.dispatcher;

import java.util.function.Consumer;

import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;

public class Dispatcher {

    private final WorkerClient[] clients;

    public Dispatcher(WorkerClient[] clients) {
        this.clients = clients;
    }

    public void submit(Event e) {
        clients[0].submit(e);
    }

    public void retrieve(long timestamp, Consumer<HourStats> callback) {
        clients[0].retrieve(timestamp, callback);
    }

}
