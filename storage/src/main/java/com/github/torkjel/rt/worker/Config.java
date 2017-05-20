package com.github.torkjel.rt.worker;

import java.io.File;
import java.util.List;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Getter
@Builder
public class Config {

    private static final int BASE_PORT = 9000;

    private static final File DATA_DIR = new File(System.getProperty("user.home"), "tapad-rt");

    @Singular
    private List<Integer> ports;

    public static Config parse(String[] args) {
        ConfigBuilder configBuilder = Config.builder();
        for (String arg : args) {
            configBuilder.port(BASE_PORT + Integer.parseInt(arg));
        }
        return configBuilder.build();
    }

    public File getDataFile(int port) {
        if ((DATA_DIR.exists() && !DATA_DIR.isDirectory()) || (!DATA_DIR.exists() && !DATA_DIR.mkdirs()))
            throw new RuntimeException("Failed to create dataDir: " + DATA_DIR);
        return new File(DATA_DIR, "data-" + port + ".mapdb");
    }

}
