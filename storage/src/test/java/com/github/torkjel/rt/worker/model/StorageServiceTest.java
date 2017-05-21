package com.github.torkjel.rt.worker.model;

import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.worker.model.InMemStorageService;
import com.github.torkjel.rt.worker.model.Event;
import com.github.torkjel.rt.worker.model.SliceStats;

import static org.assertj.core.api.Assertions.*;

public class StorageServiceTest {

    private StorageService ss;

    @Before
    public void setUp() {
        ss = new InMemStorageService();
    }

    @Test
    public void testEmpty() {
        SliceStats stats = ss.retrieve(System.currentTimeMillis() / 1000);
        assertThat(stats).isEqualTo(SliceStats.empty());
    }

    @Test
    public void testOneSlice() {

        for (int n = 1; n <= 50; n++) {
            for (int m = 0; m < 50; m++) {
                ss.store(Event.builder()
                        .slice(1)
                        .user(String.valueOf(n))
                        .action((m % 5) == 0 ? "impression" : "click")
                        .build());
            }
        }

        SliceStats stats = ss.retrieve(1);
        assertThat(stats).isEqualTo(new SliceStats(50, 2000, 500));
    }

    @Test
    public void testSeveralSlices() {

        for (int n = 0; n < 180; n++) {
            for (int m = 0; m < 60; m++) {
                ss.store(Event.builder()
                        .slice((n * 60 + m) / 3600)
                        .user(String.valueOf(n))
                        .action((m & 1) == 0 ? "impression" : "click")
                        .build());
            }
        }

        SliceStats statsHM1 = ss.retrieve(-1);
        assertThat(statsHM1).isEqualTo(new SliceStats(0, 0, 0));

        SliceStats statsH1 = ss.retrieve(0);
        assertThat(statsH1).isEqualTo(new SliceStats(60, 1800, 1800));

        SliceStats statsH2 = ss.retrieve(1);
        assertThat(statsH2).isEqualTo(new SliceStats(60, 1800, 1800));

        SliceStats statsH3 = ss.retrieve(2);
        assertThat(statsH3).isEqualTo(new SliceStats(60, 1800, 1800));

        SliceStats statsH4 = ss.retrieve(3);
        assertThat(statsH4).isEqualTo(new SliceStats(0, 0, 0));

    }

}
