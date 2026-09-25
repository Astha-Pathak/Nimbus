package com.example.scheduler.registry;

import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.WorkerInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface WorkerRegistry {
    void register(WorkerInfo worker);

    Optional<WorkerInfo> findById(String workerId);

    List<WorkerInfo> listAll();

    List<WorkerInfo> listAvailable();

    void updateHeartbeat(String workerId, WorkerState state, String currentTaskId, Instant heartbeatAt);

    void markUnhealthyIfExpired(Instant now, Duration timeout);
}
