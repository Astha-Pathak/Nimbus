package com.example.scheduler.registry;

import com.example.scheduler.domain.WorkerInfo;

import java.util.List;
import java.util.Optional;

public interface WorkerRegistry {
    void register(WorkerInfo worker);

    Optional<WorkerInfo> findById(String workerId);

    List<WorkerInfo> listAvailable();
}
