package com.github.torkjel.rt.worker.model;

import java.util.HashMap;
import java.util.Map;

public class StorageService {

    private Map<Long, Map<String, UserStats>> data = new HashMap<>();

    public void store(Event event) {
        long startOfHour = startOfHour(event.getTimestamp());
        Map<String, UserStats> hourData = data.get(startOfHour);
        if (hourData == null)
            data.put(startOfHour, hourData = new HashMap<>());
        UserStats userStats = hourData.get(event.getUser());
        if (userStats == null)
            hourData.put(event.getUser(), userStats = new UserStats());
        userStats.register(event);
    }

    public HourStats retrieve(long timestamp) {
        Map<String, UserStats> hourData = data.get(startOfHour(timestamp));
        if (hourData != null) {
            return hourData.entrySet()
                .stream()
                .map(e -> new HourStats(e.getValue()))
                .reduce((a, b) -> a.combine(b))
                .orElse(HourStats.empty());
        } else
            return HourStats.empty();
    }

    private long startOfHour(long ts) {
        // strip minutes and seconds.
        return (ts / 3600) * 3600;
    }

}
