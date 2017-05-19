package com.github.torkjel.rt.api.model;

import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.api.model.AnalyticsService;
import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;

import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class AnalyticsServiceTest {

    private AnalyticsService as;
    
    @Before
    public void setUp() {
        as = new AnalyticsService();
    }
    
    @Test
    public void testEmpty() {
        HourStats stats = as.retrieve(System.currentTimeMillis());
        assertThat(stats).isEqualTo(HourStats.empty());
    }

    @Test
    public void testOneHour() { 

        LocalDateTime thisHour = LocalDateTime.now().withMinute(0).withSecond(0);
        long ts = thisHour.toEpochSecond(ZoneOffset.UTC);
         
        for (int n = 1; n <= 50; n++) {
            for (int m = 0; m < 50; m++) {
                as.store(Event.builder()
                        .timestamp(ts + n * m)
                        .user(String.valueOf(n))
                        .action((m % 5) == 0 ? "impression" : "click")
                        .build());
            }
        }
            
        HourStats stats = as.retrieve(ts);
        assertThat(stats).isEqualTo(new HourStats(50, 2000, 500));
    }

    @Test
    public void testSeveralHours() { 

        LocalDateTime thisHour = LocalDateTime.now().withMinute(0).withSecond(0);
        long ts = thisHour.toEpochSecond(ZoneOffset.UTC);
         
        for (int n = 0; n < 180; n++) {
            for (int m = 0; m < 60; m++) {
                as.store(Event.builder()
                        .timestamp(ts + n * 60)
                        .user(String.valueOf(n))
                        .action((m & 1) == 0 ? "impression" : "click")
                        .build());
            }
        }

        HourStats statsHM1 = as.retrieve(ts - 3600);
        assertThat(statsHM1).isEqualTo(new HourStats(0, 0, 0));

        HourStats statsH1 = as.retrieve(ts);
        assertThat(statsH1).isEqualTo(new HourStats(60, 1800, 1800));

        HourStats statsH2 = as.retrieve(ts + 3600);
        assertThat(statsH2).isEqualTo(new HourStats(60, 1800, 1800));

        HourStats statsH3 = as.retrieve(ts + 3600 * 2);
        assertThat(statsH3).isEqualTo(new HourStats(60, 1800, 1800));
 
        HourStats statsH4 = as.retrieve(ts + 3600 * 3);
        assertThat(statsH4).isEqualTo(new HourStats(0, 0, 0));

    }

}
