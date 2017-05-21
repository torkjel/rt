package com.github.torkjel.rt.api.config;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class Routing {
    private final int slice;
    private final List<String> nodes;

    @JsonCreator
    public Routing(
            @JsonProperty("slice") int slice,
            @JsonProperty("nodes") List<String> nodes) {
        this.slice = slice;
        this.nodes = nodes;
    }

    public long startOfSlice(long startOfFirstSlice, long lengthOfSlice) {
        return startOfFirstSlice + lengthOfSlice * slice;
    }

    public String getNode(int routingKey) {
        routingKey = Math.abs(routingKey);
        return nodes.get(routingKey % nodes.size());
    }

}
