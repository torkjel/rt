package com.github.torkjel.rt.worker;

import java.util.HashMap;
import java.util.Map;

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

    private final Map<Integer, StorageService> storageServices = new HashMap<>();

    private WorkerMain main;
    private Config config;

    public synchronized StorageService getStorageServiceForNode(int port) {
        StorageService ss = storageServices.get(port);
        if (ss == null)
            storageServices.put(port,  ss = new StorageService());
        return ss;
    }

}
