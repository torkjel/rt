package com.github.torkjel.rt.worker.model;

public interface StorageService extends AutoCloseable {

    SliceStats store(Event event);

    SliceStats retrieve(long slice);

    public void clear();

    @Override
    default void close() { /* NO-OP */ }
}
