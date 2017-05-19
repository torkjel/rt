package com.github.torkjel.rt.api.model;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
 
@Data
@Builder
@ToString
public class Event {
    private final long timestamp;
    private final String user;
    private final String action;

    public boolean isClick() {
        return "click".equals(action);
    }
}
 