package com.example.scheduler.service;

import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.WorkerRegistry;
import com.example.scheduler.repository.TaskRepository;
import com.google.protobuf.util.JsonFormat;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class SchedulerTaskAssignmentService {

    private final TaskRepository taskRepository;
    private final WorkerRegistry workerRegistry;
    private final SchedulingStrategy schedulingStrategy;
    private final WorkerClient workerClient;

    public SchedulerTaskAssignmentService(TaskRepository taskRepository,
            WorkerRegistry workerRegistry,
            SchedulingStrategy schedulingStrategy,
            WorkerClient workerClient) {
        this.taskRepository = taskRepository;
        this.workerRegistry = workerRegistry;
        this.schedulingStrategy = schedulingStrategy;
        this.workerClient = workerClient;
    }

    public Optional<Task> assignTask(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return Optional.empty();
        }

        Task task = taskRepository.findByTaskId(taskId.trim()).orElse(null);
        if (task == null) {
            return Optional.empty();
        }

        if (task.getLifecycle() != com.example.scheduler.domain.TaskLifecycle.QUEUED) {
            return Optional.empty();
        }

        List<WorkerInfo> availableWorkers = workerRegistry.listAvailable();
        Optional<WorkerInfo> selectedWorker = schedulingStrategy.selectWorker(task, availableWorkers);
        if (selectedWorker.isEmpty()) {
            return Optional.empty();
        }

        int nextAttemptCount = task.getAttemptCount() + 1;
        WorkerTaskAssignment assignment = WorkerTaskAssignment.newBuilder()
                .setTaskId(task.getTaskId())
                .setFileId(task.getFileId())
                .setProcessorType(task.getProcessorType())
                .setAttempt(nextAttemptCount)
                .setMaxRetries(task.getMaxRetries())
                .setConfiguration(toTaskConfiguration(task.getConfiguration()))
                .build();

        WorkerAssignTaskResponse response;
        try {
            response = workerClient.assignTask(selectedWorker.get(), assignment);
        } catch (RuntimeException ex) {
            return Optional.empty();
        }

        if (response == null || !response.getAccepted()) {
            return Optional.empty();
        }

        task.setAttemptCount(nextAttemptCount);
        task.setLifecycle(com.example.scheduler.domain.TaskLifecycle.RUNNING);
        task.setAssignedWorkerId(selectedWorker.get().workerId());
        taskRepository.save(task);
        return Optional.of(task);
    }

    private TaskConfiguration toTaskConfiguration(String configuration) {
        if (configuration == null || configuration.isBlank()) {
            return TaskConfiguration.newBuilder().build();
        }

        TaskConfiguration.Builder builder = TaskConfiguration.newBuilder();
        try {
            JsonFormat.parser().merge(configuration, builder);
            return builder.build();
        } catch (Exception ignored) {
            return TaskConfiguration.newBuilder().build();
        }
    }
}
