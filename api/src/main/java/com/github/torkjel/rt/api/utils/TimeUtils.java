package com.github.torkjel.rt.api.utils;

public class TimeUtils {

    public static long startOfHour(long timestamp) {
        return timestamp / 3600 * 3600;
    }

}
