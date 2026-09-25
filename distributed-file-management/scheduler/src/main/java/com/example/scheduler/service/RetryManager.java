package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RetryManager {

    private static final long BASE_DELAY_MS = 100L;

    private final TaskRepository taskRepository;
    private final SchedulerTaskAssignmentService assignmentService;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Set<String>> scheduledRetries = new ConcurrentHashMap<>();
    private final Map<String, Long> retryDelays = new ConcurrentHashMap<>();

    public RetryManager(TaskRepository taskRepository, SchedulerTaskAssignmentService assignmentService) {
        this(taskRepository, assignmentService, BASE_DELAY_MS);
    }

    public RetryManager(TaskRepository taskRepository,
            SchedulerTaskAssignmentService assignmentService,
            long baseDelayMs) {
        this.taskRepository = taskRepository;
        this.assignmentService = assignmentService;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.retryDelays.put("baseDelayMs", baseDelayMs);
    }

    public boolean shouldRetry(Task task) {
        if (task == null) {
            return false;
        }
        int totalAllowedAttempts = 1 + task.getMaxRetries();
        return task.getAttemptCount() < totalAllowedAttempts;
    }

    public long calculateDelayMs(int retryNumber) {
        long base = Math.max(retryDelays.getOrDefault("baseDelayMs", BASE_DELAY_MS), BASE_DELAY_MS);
        return base * (1L << Math.max(0, retryNumber - 1));
    }

    public void scheduleRetry(Task task) {
        if (task == null) {
            return;
        }

        if (!shouldRetry(task)) {
            task.setLifecycle(TaskLifecycle.FAILED);
            task.setAssignedWorkerId(null);
            taskRepository.save(task);
            return;
        }

        task.setLifecycle(TaskLifecycle.QUEUED);
        task.setAssignedWorkerId(null);
        taskRepository.save(task);

        int retryNumber = task.getAttemptCount();
        long delayMs = calculateDelayMs(retryNumber);
        scheduledRetries.computeIfAbsent(task.getTaskId(), key -> ConcurrentHashMap.newKeySet());
        scheduledRetries.get(task.getTaskId()).add(String.valueOf(retryNumber));

        scheduler.schedule(() -> triggerScheduledRetry(task.getTaskId()), delayMs, TimeUnit.MILLISECONDS);
    }

    public int getScheduledRetryCount(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return 0;
        }
        return scheduledRetries.getOrDefault(taskId, Set.of()).size();
    }

    public void triggerScheduledRetry(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return;
        }

        if (assignmentService == null) {
            return;
        }

        Task task = taskRepository.findByTaskId(taskId).orElse(null);
        if (task == null || task.getLifecycle() != TaskLifecycle.QUEUED) {
            return;
        }

        assignmentService.assignTask(taskId);
    }
}
