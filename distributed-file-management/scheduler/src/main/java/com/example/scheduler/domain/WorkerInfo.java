package com.example.scheduler.domain;

import com.example.nimbus.v1.WorkerState;

import java.time.Instant;
import java.util.List;

public record WorkerInfo(
        String workerId,
        List<String> supportedProcessorTypes,
        int capacity,
        String host,
        WorkerState state,
        Instant registeredAt,
        Instant lastHeartbeatAt,
        String currentTaskId) {

    public WorkerInfo(
            String workerId,
            List<String> supportedProcessorTypes,
            int capacity,
            String host,
            WorkerState state,
            Instant registeredAt,
            Instant lastHeartbeatAt) {
        this(workerId, supportedProcessorTypes, capacity, host, state, registeredAt, lastHeartbeatAt, null);
    }
}
