package com.example.scheduler.registry;

import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.WorkerInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryWorkerRegistry implements WorkerRegistry {

    private final ConcurrentMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();

    @Override
    public void register(WorkerInfo worker) {
        if (worker == null || worker.workerId() == null || worker.workerId().isBlank()) {
            throw new IllegalArgumentException("Worker ID must not be blank");
        }

        workers.compute(worker.workerId(), (id, existing) -> {
            if (existing == null) {
                return worker;
            }

            return new WorkerInfo(
                    id,
                    new ArrayList<>(worker.supportedProcessorTypes()),
                    worker.capacity(),
                    worker.host(),
                    worker.state(),
                    worker.registeredAt(),
                    worker.lastHeartbeatAt(),
                    worker.currentTaskId());
        });
    }

    @Override
    public Optional<WorkerInfo> findById(String workerId) {
        if (workerId == null || workerId.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(workers.get(workerId.trim()));
    }

    @Override
    public List<WorkerInfo> listAvailable() {
        return workers.values().stream()
                .filter(worker -> worker.state() == WorkerState.AVAILABLE)
                .toList();
    }

    @Override
    public void updateHeartbeat(String workerId, WorkerState state, String currentTaskId, java.time.Instant heartbeatAt) {
        if (workerId == null || workerId.isBlank()) {
            return;
        }

        workers.computeIfPresent(workerId.trim(), (id, existing) -> new WorkerInfo(
                existing.workerId(),
                existing.supportedProcessorTypes(),
                existing.capacity(),
                existing.host(),
                state == null ? existing.state() : state,
                existing.registeredAt(),
                heartbeatAt == null ? existing.lastHeartbeatAt() : heartbeatAt,
                currentTaskId == null || currentTaskId.isBlank() ? null : currentTaskId.trim()));
    }
}
