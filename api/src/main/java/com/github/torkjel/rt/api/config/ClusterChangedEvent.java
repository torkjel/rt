package com.github.torkjel.rt.api.config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ClusterChangedEvent {
    private final Cluster cluster;
}
