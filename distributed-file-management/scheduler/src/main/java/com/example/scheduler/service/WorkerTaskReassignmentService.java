package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class WorkerTaskReassignmentService {

    private static final String WORKER_FAILURE_ERROR_CODE = "WORKER_FAILED";
    private static final String WORKER_FAILURE_MESSAGE =
            "Task could not be recovered because the assigned worker became unhealthy after the allowed attempts were exhausted.";

    private final TaskRepository taskRepository;
    private final SchedulerTaskAssignmentService assignmentService;

    public WorkerTaskReassignmentService(TaskRepository taskRepository,
            SchedulerTaskAssignmentService assignmentService) {
        this.taskRepository = taskRepository;
        this.assignmentService = assignmentService;
    }

    @Transactional
    public void reassignTasksForUnhealthyWorker(String workerId) {
        if (workerId == null || workerId.isBlank()) {
            return;
        }

        List<Task> runningTasks = taskRepository.findByLifecycle(TaskLifecycle.RUNNING);
        if (runningTasks == null || runningTasks.isEmpty()) {
            return;
        }

        for (Task task : runningTasks) {
            if (task == null || task.getAssignedWorkerId() == null || !workerId.equals(task.getAssignedWorkerId())) {
                continue;
            }

            if (task.getLifecycle() != TaskLifecycle.RUNNING) {
                continue;
            }

            int totalAllowedAttempts = 1 + task.getMaxRetries();
            if (task.getAttemptCount() < totalAllowedAttempts) {
                task.setLifecycle(TaskLifecycle.QUEUED);
                task.setAssignedWorkerId(null);
                task.setErrorCode(null);
                task.setErrorMessage(null);
                taskRepository.save(task);

                if (assignmentService != null) {
                    assignmentService.assignTask(task.getTaskId());
                }
                continue;
            }

            task.setLifecycle(TaskLifecycle.FAILED);
            task.setAssignedWorkerId(null);
            task.setErrorCode(WORKER_FAILURE_ERROR_CODE);
            task.setErrorMessage(WORKER_FAILURE_MESSAGE);
            taskRepository.save(task);
        }
    }
}
