package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.WorkerInfo;

import java.util.List;
import java.util.Optional;

public interface SchedulingStrategy {

    Optional<WorkerInfo> selectWorker(Task task, List<WorkerInfo> availableWorkers);
}
