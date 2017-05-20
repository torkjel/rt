package com.github.torkjel.rt.worker;

import java.util.List;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Getter
@Builder
public class Config {

    private static final int BASE_PORT = 9000;

    @Singular
    private List<Integer> ports;

    public static Config parse(String[] args) {
        ConfigBuilder configBuilder = Config.builder();
        for (String arg : args) {
            configBuilder.port(BASE_PORT + Integer.parseInt(arg));
        }
        return configBuilder.build();
    }
}
