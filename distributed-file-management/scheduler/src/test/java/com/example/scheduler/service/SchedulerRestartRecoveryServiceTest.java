package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SchedulerRestartRecoveryServiceTest {

    @Test
    void shouldRecoverRunningTaskToQueuedWhenAttemptsRemain() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-recover", "file-recover", "ocr", "{}", TaskLifecycle.RUNNING, "worker-a", 1, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerRestartRecoveryService service = new SchedulerRestartRecoveryService(repository);
        service.recover();

        assertEquals(TaskLifecycle.QUEUED, task.getLifecycle());
        assertNull(task.getAssignedWorkerId());
        assertEquals(1, task.getAttemptCount());
        verify(repository).save(task);
    }

    @Test
    void shouldFailRunningTaskWhenAttemptsAreExhausted() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-exhausted", "file-exhausted", "ocr", "{}", TaskLifecycle.RUNNING, "worker-b", 3, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerRestartRecoveryService service = new SchedulerRestartRecoveryService(repository);
        service.recover();

        assertEquals(TaskLifecycle.FAILED, task.getLifecycle());
        assertEquals("TASK_RECOVERY_EXHAUSTED", task.getErrorCode());
        assertTrue(task.getErrorMessage().contains("recovery"));
    }

    @Test
    void shouldLeaveQueuedCompletedAndFailedTasksUntouched() {
        TaskRepository repository = mock(TaskRepository.class);
        Task queued = new Task("req-queued", "file-queued", "ocr", "{}", TaskLifecycle.QUEUED, null, 1, 2, null, null);
        Task completed = new Task("req-completed", "file-completed", "ocr", "{}", TaskLifecycle.COMPLETED, "worker-c", 2, 2, null, null);
        Task failed = new Task("req-failed", "file-failed", "ocr", "{}", TaskLifecycle.FAILED, "worker-d", 3, 2, "ERR", "msg");
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of());

        SchedulerRestartRecoveryService service = new SchedulerRestartRecoveryService(repository);
        service.recover();

        assertEquals(TaskLifecycle.QUEUED, queued.getLifecycle());
        assertEquals(TaskLifecycle.COMPLETED, completed.getLifecycle());
        assertEquals(TaskLifecycle.FAILED, failed.getLifecycle());
        verify(repository, never()).save(any(Task.class));
    }

    @Test
    void shouldBeIdempotentAcrossMultipleRecoveryRuns() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-idempotent", "file-idempotent", "ocr", "{}", TaskLifecycle.RUNNING, "worker-e", 1, 2, null, null);
        when(repository.findByLifecycle(TaskLifecycle.RUNNING)).thenReturn(List.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerRestartRecoveryService service = new SchedulerRestartRecoveryService(repository);
        service.recover();
        service.recover();

        assertEquals(TaskLifecycle.QUEUED, task.getLifecycle());
        assertEquals(1, task.getAttemptCount());
        verify(repository, times(1)).save(task);
    }
}
