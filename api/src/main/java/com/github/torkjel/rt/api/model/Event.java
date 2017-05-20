package com.github.torkjel.rt.api.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import static com.github.torkjel.rt.api.utils.HashUtil.hash;

@Data
@ToString
public class Event {
    private final long timestamp;
    private final long hourStart;
    private final String user;
    private final String action;

    @Builder
    private Event(long timestamp, String user, String action) {
        this.timestamp = timestamp;
        this.hourStart = timestamp / 3600 * 3600;
        this.user = user;
        this.action = action;
    }

    public boolean isClick() {
        return "click".equals(action);
    }

    public String toUrlQueryPart() {
        return "timestamp=" + timestamp + "&user=" + user + "&" + action;
    }

    public Event anonymized() {
        return Event.builder()
                .timestamp(timestamp)
                .action(action)
                .user(hash(hourStart, user))
                .build();
    }
}
