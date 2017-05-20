package com.github.torkjel.rt.worker.model;

public interface StorageService extends AutoCloseable {

    HourStats store(Event event);

    HourStats retrieve(long timestamp);

    public void clear();

    @Override
    default void close() { /* NO-OP */ }
}
