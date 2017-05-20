package com.github.torkjel.rt.worker.model;

import java.io.Serializable;

import lombok.Data;

@Data
public class UserStats implements Serializable {
    private final int clicks;
    private final int impressions;

    public static UserStats empty() {
        return new UserStats(0, 0);
    }

    public UserStats udpate(Event e) {
        return new UserStats(
                clicks + (e.isClick() ? 1 : 0),
                impressions + (e.isClick() ? 0 : 1));
    }
}
