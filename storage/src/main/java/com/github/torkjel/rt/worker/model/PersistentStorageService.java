package com.github.torkjel.rt.worker.model;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import lombok.extern.log4j.Log4j;

@Log4j
public class PersistentStorageService implements StorageService, AutoCloseable {

    private final DB db;

    private final Map<Long, Map<String, UserStats>> data = new HashMap<>();

    private final Map<Long, HourStats> summaries;

    @SuppressWarnings("unchecked")
    public PersistentStorageService(File dataFile) {
        db = DBMaker.fileDB(dataFile).fileMmapEnableIfSupported().make();
        summaries = (Map<Long, HourStats>)db.hashMap("summaries").createOrOpen();
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized HourStats store(Event event) {
        log.info("store start");
        long startOfHour = startOfHour(event.getTimestamp());
        Map<String, UserStats> hourData = data.get(startOfHour);
        if (hourData == null) {
            hourData = (Map<String, UserStats>)db.hashMap("data-" + startOfHour).createOrOpen();
            data.put(startOfHour, hourData);
        }

        HourStats summary = getSummary(startOfHour);

        UserStats userStats = hourData.get(event.getUser());

        if (userStats == null) {
            userStats = UserStats.empty();
            summary = summary.addEventForNewUser(event);
        } else {
            summary = summary.addEventForKnownUser(event);
        }
        hourData.put(event.getUser(), userStats.udpate(event));

        updateSummary(startOfHour, summary);
        log.info("store end");
        return summary;
    }

    @Override
    public synchronized void clear() {
        if (!db.isClosed()) {
            data.values().forEach(Map::clear);
            summaries.clear();
        } else {
            log.warn("DB closed");
        }
    }

    @Override
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

    private synchronized HourStats getSummary(long startOfHour) {
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

    @Override
    public void close() {
        if (!db.isClosed())
            db.close();
    }
}
