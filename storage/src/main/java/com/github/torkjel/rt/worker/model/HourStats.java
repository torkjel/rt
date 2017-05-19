package com.github.torkjel.rt.worker.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HourStats {

    private final long uniqueUsers;
    private final long clicks;
    private final long impressions;

    public static HourStats empty() {
        return new HourStats(0, 0, 0);
    }

    public HourStats(UserStats userStats) {
        uniqueUsers = 1;
        clicks = userStats.getClicks().get();
        impressions = userStats.getImpressions().get();
    }

    public HourStats combine(HourStats that) {
        return new HourStats(
                this.uniqueUsers + that.getUniqueUsers(),
                this.clicks + that.getClicks(),
                this.impressions + that.getImpressions());
    }

    public String toString() {
        // Typo "unqiue", as per spec.
        return
                "unqiue_users," + uniqueUsers + "\n" +
                "clicks," + clicks + "\n" +
                "impressions," + impressions + "\n";
    }
}
