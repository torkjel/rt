package com.github.torkjel.rt.worker.model;

import java.util.HashMap;
import java.util.Map;

public class StorageService {

    private Map<Long, Map<String, UserStats>> data = new HashMap<>();

    private Map<Long, HourStats> summaries = new HashMap<>();

    public synchronized HourStats store(Event event) {
        long startOfHour = startOfHour(event.getTimestamp());
        Map<String, UserStats> hourData = data.get(startOfHour);
        if (hourData == null)
            data.put(startOfHour, hourData = new HashMap<>());

        HourStats summary = getSummary(startOfHour);

        UserStats userStats = hourData.get(event.getUser());
        if (userStats == null) {
            hourData.put(event.getUser(), userStats = new UserStats());
            summary = summary.addEventForNewUser(event);
        } else {
            summary = summary.addEventForKnownUser(event);
        }

        userStats.register(event);

        updateSummary(startOfHour, summary);
        return summary;
    }

    public synchronized HourStats retrieve(long timestamp) {
        return getSummary(startOfHour(timestamp));
    }

    // not used for now.
    public synchronized HourStats retrieveFull(long timestamp) {
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

    private HourStats getSummary(long startOfHour) {
        HourStats summary = summaries.get(startOfHour);
        if (summary == null)
            summaries.put(startOfHour, summary = HourStats.empty());
        return summary;
    }

    private void updateSummary(long startOfHour, HourStats updated) {
        summaries.put(startOfHour, updated);
    }

    private long startOfHour(long ts) {
        // strip minutes and seconds.
        return (ts / 3600) * 3600;
    }


}
