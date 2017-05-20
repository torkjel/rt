package com.github.torkjel.rt.api.model;

import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class HourStats {

    private final long uniqueUsers;
    private final long clicks;
    private final long impressions;

    public HourStats combine(HourStats that) {
        return new HourStats(
                this.uniqueUsers + that.getUniqueUsers(),
                this.clicks + that.getClicks(),
                this.impressions + that.getImpressions());
    }

    public static HourStats empty() {
        return new HourStats(0, 0, 0);
    }

    public static HourStats parse(String message) {
        String[] lines = message.split("\n");
        HourStatsBuilder builder = HourStats.builder();
        Function<String, Long> parser = s -> Long.parseLong(s.substring(s.lastIndexOf(',')+1));
        for (String line : lines)
            if (line.startsWith("unqiue_users"))
                builder.uniqueUsers(parser.apply(line));
            else if (line.startsWith("clicks"))
                builder.clicks(parser.apply(line));
            else if (line.startsWith("impressions"))
                builder.impressions(parser.apply(line));
        return builder.build();
    }

    public String toString() {
        // Typo "unqiue", as per spec.
        return
                "unqiue_users," + uniqueUsers + "\n" +
                "clicks," + clicks + "\n" +
                "impressions," + impressions + "\n";
    }
}
