package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.WorkerRegistry;

import java.util.List;
import java.util.Optional;

public class SchedulerWorkerSelectionService {

    private final WorkerRegistry workerRegistry;
    private final SchedulingStrategy schedulingStrategy;

    public SchedulerWorkerSelectionService(WorkerRegistry workerRegistry,
            SchedulingStrategy schedulingStrategy) {
        this.workerRegistry = workerRegistry;
        this.schedulingStrategy = schedulingStrategy;
    }

    public Optional<WorkerInfo> selectWorker(Task task) {
        if (task == null) {
            return Optional.empty();
        }

        List<WorkerInfo> availableWorkers = workerRegistry.listAvailable();
        return schedulingStrategy.selectWorker(task, availableWorkers);
    }
}
