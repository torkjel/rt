package com.github.torkjel.rt.api.model;

import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class HoursStatsTest {
    
    @Test
    public void testSerializeParseRoundtrip() {
        HourStats stats = HourStats.builder()
            .uniqueUsers(1234)
            .clicks(2345)
            .impressions(4567)
            .build();

        assertThat(HourStats.parse(stats.toString())).isEqualTo(stats);
        
    }
}
