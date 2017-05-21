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

    private final Map<Long, SliceStats> summaries;

    @SuppressWarnings("unchecked")
    public PersistentStorageService(File dataFile) {
        db = DBMaker.fileDB(dataFile).fileMmapEnableIfSupported().make();
        summaries = (Map<Long, SliceStats>)db.hashMap("summaries").createOrOpen();
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized SliceStats store(Event event) {
        log.info("store start");
        long slice = event.getSlice();
        Map<String, UserStats> sliceData = data.get(slice);
        if (sliceData == null) {
            sliceData = (Map<String, UserStats>)db.hashMap("data-" + slice).createOrOpen();
            data.put(slice, sliceData);
        }

        SliceStats summary = getSummary(slice);

        UserStats userStats = sliceData.get(event.getUser());

        if (userStats == null) {
            userStats = UserStats.empty();
            summary = summary.addEventForNewUser(event);
        } else {
            summary = summary.addEventForKnownUser(event);
        }
        sliceData.put(event.getUser(), userStats.udpate(event));

        updateSummary(slice, summary);
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
    public synchronized SliceStats retrieve(long slice) {
        return getSummary(slice);
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

    private synchronized SliceStats getSummary(long slice) {
        SliceStats summary = summaries.get(slice);
        if (summary == null)
            summaries.put(slice, summary = SliceStats.empty());
        return summary;
    }

    private void updateSummary(long slice, SliceStats updated) {
        summaries.put(slice, updated);
    }

    @Override
    public void close() {
        if (!db.isClosed())
            db.close();
    }
}
