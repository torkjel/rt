package com.github.torkjel.rt.api;

import com.github.torkjel.rt.api.model.AnalyticsService;

public class Services {

    private static final Services INSTANCE = new Services();
    
    public static Services instance() {
        return INSTANCE;
    }

    private final AnalyticsService analyticsService = new AnalyticsService();
    
    public AnalyticsService getAnalyticsService() {
        return analyticsService;
    }

    private Main main;
    
    public Main getMain() {
        return main;
    }

    public void setMain(Main main) {
        this.main = main;
    }
    
}
