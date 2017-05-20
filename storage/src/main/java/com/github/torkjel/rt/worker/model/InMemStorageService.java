package com.github.torkjel.rt.worker.model;

import java.util.HashMap;
import java.util.Map;

public class InMemStorageService implements StorageService {

    private Map<Long, Map<String, UserStats>> data = new HashMap<>();

    private Map<Long, HourStats> summaries = new HashMap<>();

    @Override
    public synchronized HourStats store(Event event) {
        long startOfHour = startOfHour(event.getTimestamp());
        Map<String, UserStats> hourData = data.get(startOfHour);
        if (hourData == null)
            data.put(startOfHour, hourData = new HashMap<>());

        HourStats summary = getSummary(startOfHour);

        UserStats userStats = hourData.get(event.getUser());
        if (userStats == null) {
            userStats = UserStats.empty();
            summary = summary.addEventForNewUser(event);
        } else {
            summary = summary.addEventForKnownUser(event);
        }
        hourData.put(event.getUser(), userStats);

        updateSummary(startOfHour, summary);
        return summary;
    }

    @Override
    public synchronized HourStats retrieve(long timestamp) {
        return getSummary(startOfHour(timestamp));
    }

    @Override
    public synchronized void clear() {
        data.clear();
        summaries.clear();
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
