package com.github.torkjel.rt.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import static com.github.torkjel.rt.api.utils.HashUtil.hash;

@Data
@ToString
@AllArgsConstructor
public class Event {
    private final long timestamp;
    private final long slice;
    private final String user;
    private final String action;

    public long getTimestamp() {
        return timestamp;
    }

    @Builder
    private Event(long timestamp, String user, String action) {
        this(timestamp, -1, user, action);
    }

    public boolean isClick() {
        return "click".equals(action);
    }

    public String toUrlQueryPart() {
        return "slice=" + slice + "&user=" + user + "&" + action;
    }

    public Event anonymized(long slice) {
        return new Event(0, slice, hash(slice, user), action);
    }

    public int getRoutingKey() {
        // TODO: this may be naive.
        // Don't know if this routing key MOD the number of workers
        // will result in an even distribution.
        return user.hashCode();
    }
}
