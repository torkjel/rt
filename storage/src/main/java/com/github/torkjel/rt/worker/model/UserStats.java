package com.github.torkjel.rt.worker.model;

import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;

@Data
public class UserStats {
    private AtomicInteger clicks = new AtomicInteger();
    private AtomicInteger impressions = new AtomicInteger();

    public void click() {
        clicks.incrementAndGet();
    }

    public void impress() {
        impressions.incrementAndGet();
    }

    public void register(Event e) {
        if (e.isClick())
            click();
        else
            impress();
    }
}
