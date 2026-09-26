package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.WorkerInfo;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RoundRobinSchedulingStrategy implements SchedulingStrategy {

    private final AtomicInteger nextIndex = new AtomicInteger(0);

    @Override
    public Optional<WorkerInfo> selectWorker(Task task, List<WorkerInfo> availableWorkers) {
        if (task == null || task.getProcessorType() == null || task.getProcessorType().isBlank()) {
            return Optional.empty();
        }
        if (availableWorkers == null || availableWorkers.isEmpty()) {
            return Optional.empty();
        }

        List<WorkerInfo> eligibleWorkers = availableWorkers.stream()
                .filter(worker -> worker != null)
                .filter(worker -> worker.state() == WorkerState.AVAILABLE)
                .filter(worker -> supportsProcessor(worker, task.getProcessorType()))
                .sorted(Comparator.comparing(WorkerInfo::workerId))
                .toList();

        if (eligibleWorkers.isEmpty()) {
            return Optional.empty();
        }

        int size = eligibleWorkers.size();
        int selectedIndex = Math.floorMod(nextIndex.getAndUpdate(index -> (index + 1) % size), size);
        return Optional.of(eligibleWorkers.get(selectedIndex));
    }

    private boolean supportsProcessor(WorkerInfo worker, String processorType) {
        if (worker.supportedProcessorTypes() == null || worker.supportedProcessorTypes().isEmpty()) {
            return false;
        }

        String normalizedProcessorType = processorType.trim();
        return worker.supportedProcessorTypes().stream()
                .filter(value -> value != null)
                .anyMatch(value -> value.equalsIgnoreCase(normalizedProcessorType));
    }
}
