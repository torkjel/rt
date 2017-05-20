package com.github.torkjel.rt.worker.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HourStats implements Serializable {

    private final long uniqueUsers;
    private final long clicks;
    private final long impressions;

    public static HourStats empty() {
        return new HourStats(0, 0, 0);
    }

    public HourStats(UserStats userStats) {
        uniqueUsers = 1;
        clicks = userStats.getClicks();
        impressions = userStats.getImpressions();
    }

    public HourStats combine(HourStats that) {
        return new HourStats(
                this.uniqueUsers + that.getUniqueUsers(),
                this.clicks + that.getClicks(),
                this.impressions + that.getImpressions());
    }

    public HourStats addEventForNewUser(Event event) {
        return new HourStats(
                this.uniqueUsers + 1,
                this.clicks + ("click".equals(event.getAction()) ? 1 : 0),
                this.impressions + ("impression".equals(event.getAction()) ? 1 : 0));
    }

    public HourStats addEventForKnownUser(Event event) {
        return new HourStats(
                this.uniqueUsers,
                this.clicks + ("click".equals(event.getAction()) ? 1 : 0),
                this.impressions + ("impression".equals(event.getAction()) ? 1 : 0));
    }


    public String toString() {
        // Typo "unqiue", as per spec.
        return
                "unqiue_users," + uniqueUsers + "\n" +
                "clicks," + clicks + "\n" +
                "impressions," + impressions + "\n";
    }
}
