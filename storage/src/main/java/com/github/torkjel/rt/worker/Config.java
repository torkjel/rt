package com.github.torkjel.rt.worker;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Config {

    private static final int BASE_PORT = 9000;

    private final int port;

    public static Config parse(String[] args) {
        if (args.length >= 1)
            return forNode(Integer.parseInt(args[0]));
        throw new IllegalArgumentException("Need node number argument");
    }

    public static Config forNode(int node) {
        return Config.builder().port(BASE_PORT + node).build();
    }

}
