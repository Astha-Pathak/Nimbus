package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class WorkerTaskReassignmentServiceTest {

    @Test
    void shouldRequeueRunningTaskWhenRetriesRemain() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-a", "file-a", "ocr", "{}", TaskLifecycle.RUNNING, "worker-a", 1, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerTaskAssignmentService assignmentService = mock(SchedulerTaskAssignmentService.class);
        WorkerTaskReassignmentService service = new WorkerTaskReassignmentService(repository, assignmentService);

        service.reassignTasksForUnhealthyWorker("worker-a");

        assertEquals(TaskLifecycle.QUEUED, task.getLifecycle());
        assertNull(task.getAssignedWorkerId());
        assertEquals(1, task.getAttemptCount());
        verify(assignmentService).assignTask(task.getTaskId());
    }

    @Test
    void shouldFailTaskWhenAllowedAttemptsAreExhausted() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-b", "file-b", "ocr", "{}", TaskLifecycle.RUNNING, "worker-b", 3, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        WorkerTaskReassignmentService service = new WorkerTaskReassignmentService(repository, mock(SchedulerTaskAssignmentService.class));

        service.reassignTasksForUnhealthyWorker("worker-b");

        assertEquals(TaskLifecycle.FAILED, task.getLifecycle());
        assertEquals("WORKER_FAILED", task.getErrorCode());
        assertTrue(task.getErrorMessage().contains("worker became unhealthy"));
    }

    @Test
    void shouldIgnoreCompletedTasks() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-c", "file-c", "ocr", "{}", TaskLifecycle.COMPLETED, "worker-c", 1, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(task));

        WorkerTaskReassignmentService service = new WorkerTaskReassignmentService(repository, mock(SchedulerTaskAssignmentService.class));
        service.reassignTasksForUnhealthyWorker("worker-c");

        assertEquals(TaskLifecycle.COMPLETED, task.getLifecycle());
        verify(repository, never()).save(any(Task.class));
    }

    @Test
    void shouldIgnoreQueuedTasksAndUnassignedWorkers() {
        TaskRepository repository = mock(TaskRepository.class);
        Task queuedTask = new Task("req-d", "file-d", "ocr", "{}", TaskLifecycle.QUEUED, null, 1, 2, null, null);
        Task otherAssigned = new Task("req-e", "file-e", "ocr", "{}", TaskLifecycle.RUNNING, "worker-e", 1, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(queuedTask, otherAssigned));

        WorkerTaskReassignmentService service = new WorkerTaskReassignmentService(repository, mock(SchedulerTaskAssignmentService.class));
        service.reassignTasksForUnhealthyWorker("worker-a");

        assertEquals(TaskLifecycle.QUEUED, queuedTask.getLifecycle());
        assertEquals(TaskLifecycle.RUNNING, otherAssigned.getLifecycle());
        verify(repository, never()).save(any(Task.class));
    }

    @Test
    void shouldNotReprocessAlreadyRequeuedTask() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-f", "file-f", "ocr", "{}", TaskLifecycle.RUNNING, "worker-f", 1, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> {
            Task saved = invocation.getArgument(0);
            saved.setLifecycle(TaskLifecycle.QUEUED);
            return saved;
        });

        WorkerTaskReassignmentService service = new WorkerTaskReassignmentService(repository, mock(SchedulerTaskAssignmentService.class));
        service.reassignTasksForUnhealthyWorker("worker-f");
        service.reassignTasksForUnhealthyWorker("worker-f");

        assertEquals(TaskLifecycle.QUEUED, task.getLifecycle());
        verify(repository, times(1)).save(task);
    }
}
