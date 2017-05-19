package com.github.torkjel.rt.worker.model;

import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.worker.model.StorageService;
import com.github.torkjel.rt.worker.model.Event;
import com.github.torkjel.rt.worker.model.HourStats;

import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class StorageServiceTest {

    private StorageService ss;

    @Before
    public void setUp() {
        ss = new StorageService();
    }

    @Test
    public void testEmpty() {
        HourStats stats = ss.retrieve(System.currentTimeMillis());
        assertThat(stats).isEqualTo(HourStats.empty());
    }

    @Test
    public void testOneHour() {

        LocalDateTime thisHour = LocalDateTime.now().withMinute(0).withSecond(0);
        long ts = thisHour.toEpochSecond(ZoneOffset.UTC);

        for (int n = 1; n <= 50; n++) {
            for (int m = 0; m < 50; m++) {
                ss.store(Event.builder()
                        .timestamp(ts + n * m)
                        .user(String.valueOf(n))
                        .action((m % 5) == 0 ? "impression" : "click")
                        .build());
            }
        }

        HourStats stats = ss.retrieve(ts);
        assertThat(stats).isEqualTo(new HourStats(50, 2000, 500));
    }

    @Test
    public void testSeveralHours() {

        LocalDateTime thisHour = LocalDateTime.now().withMinute(0).withSecond(0);
        long ts = thisHour.toEpochSecond(ZoneOffset.UTC);

        for (int n = 0; n < 180; n++) {
            for (int m = 0; m < 60; m++) {
                ss.store(Event.builder()
                        .timestamp(ts + n * 60)
                        .user(String.valueOf(n))
                        .action((m & 1) == 0 ? "impression" : "click")
                        .build());
            }
        }

        HourStats statsHM1 = ss.retrieve(ts - 3600);
        assertThat(statsHM1).isEqualTo(new HourStats(0, 0, 0));

        HourStats statsH1 = ss.retrieve(ts);
        assertThat(statsH1).isEqualTo(new HourStats(60, 1800, 1800));

        HourStats statsH2 = ss.retrieve(ts + 3600);
        assertThat(statsH2).isEqualTo(new HourStats(60, 1800, 1800));

        HourStats statsH3 = ss.retrieve(ts + 3600 * 2);
        assertThat(statsH3).isEqualTo(new HourStats(60, 1800, 1800));

        HourStats statsH4 = ss.retrieve(ts + 3600 * 3);
        assertThat(statsH4).isEqualTo(new HourStats(0, 0, 0));

    }

}
