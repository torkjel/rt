package com.github.torkjel.rt.worker.model;

import java.util.HashMap;
import java.util.Map;

public class InMemStorageService implements StorageService {

    private Map<Long, Map<String, UserStats>> data = new HashMap<>();

    private Map<Long, SliceStats> summaries = new HashMap<>();

    @Override
    public synchronized SliceStats store(Event event) {
        long slice = event.getSlice();
        Map<String, UserStats> sliceData = data.get(slice);
        if (sliceData == null)
            data.put(slice, sliceData = new HashMap<>());

        SliceStats summary = getSummary(slice);

        UserStats userStats = sliceData.get(event.getUser());
        if (userStats == null) {
            userStats = UserStats.empty();
            summary = summary.addEventForNewUser(event);
        } else {
            summary = summary.addEventForKnownUser(event);
        }
        sliceData.put(event.getUser(), userStats);

        updateSummary(slice, summary);
        return summary;
    }

    @Override
    public synchronized SliceStats retrieve(long slice) {
        return getSummary(slice);
    }

    @Override
    public synchronized void clear() {
        data.clear();
        summaries.clear();
    }

    // not used for now.
    public synchronized SliceStats retrieveFull(long slice) {
        Map<String, UserStats> sliceData = data.get(slice);
        if (sliceData != null) {
            return sliceData.entrySet()
                .stream()
                .map(e -> new SliceStats(e.getValue()))
                .reduce((a, b) -> a.combine(b))
                .orElse(SliceStats.empty());
        } else
            return SliceStats.empty();
    }

    private SliceStats getSummary(long slice) {
        SliceStats summary = summaries.get(slice);
        if (summary == null)
            summaries.put(slice, summary = SliceStats.empty());
        return summary;
    }

    private void updateSummary(long slice, SliceStats updated) {
        summaries.put(slice, updated);
    }
}
