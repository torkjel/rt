package com.github.torkjel.rt.api;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Config {

    private static final int BASE_PORT = 8000;

    private final int port;
    private final String[] workers = new String[] {"http://localhost:9000/worker"};

    public static Config parse(String[] args) {
        if (args.length >= 1)
            return forNode(Integer.parseInt(args[0]));
        throw new IllegalArgumentException("Need node number argument");
    }

    public static Config forNode(int node) {
        return Config.builder().port(BASE_PORT + node).build();
    }

}
