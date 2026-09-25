package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class SchedulerRestartRecoveryService {

    private static final String RECOVERY_ERROR_CODE = "TASK_RECOVERY_EXHAUSTED";
    private static final String RECOVERY_ERROR_MESSAGE =
            "Task recovery failed because its execution was interrupted by a scheduler restart and the allowed attempts were exhausted.";

    private final TaskRepository taskRepository;

    public SchedulerRestartRecoveryService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
    @Transactional
    public void recover() {
        if (taskRepository == null) {
            return;
        }

        List<Task> runningTasks = taskRepository.findByLifecycle(TaskLifecycle.RUNNING);
        if (runningTasks == null || runningTasks.isEmpty()) {
            return;
        }

        for (Task task : runningTasks) {
            if (task == null || task.getLifecycle() != TaskLifecycle.RUNNING) {
                continue;
            }

            int totalAllowedAttempts = 1 + task.getMaxRetries();
            if (task.getAttemptCount() < totalAllowedAttempts) {
                task.setLifecycle(TaskLifecycle.QUEUED);
                task.setAssignedWorkerId(null);
                task.setErrorCode(null);
                task.setErrorMessage(null);
                taskRepository.save(task);
                continue;
            }

            task.setLifecycle(TaskLifecycle.FAILED);
            task.setAssignedWorkerId(null);
            task.setErrorCode(RECOVERY_ERROR_CODE);
            task.setErrorMessage(RECOVERY_ERROR_MESSAGE);
            taskRepository.save(task);
        }
    }
}
