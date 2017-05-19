package com.github.torkjel.rt.worker;

import com.github.torkjel.rt.worker.model.StorageService;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Services {

    private static final Services INSTANCE = new Services();

    public static Services instance() {
        return INSTANCE;
    }

    private final StorageService analyticsService = new StorageService();
    private WorkerMain main;
    private Config config;

}
