package com.example.scheduler.service;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RetryManagerTest {

    @Test
    void shouldScheduleRetryWhenAttemptsRemain() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-r1", "file-r1", "ocr", "{}", TaskLifecycle.RUNNING, "worker-1", 1, 1, null, null);
        when(repository.findByTaskId(task.getTaskId())).thenReturn(Optional.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerTaskAssignmentService assignmentService = mock(SchedulerTaskAssignmentService.class);
        RetryManager manager = new RetryManager(repository, assignmentService, 1L);

        assertTrue(manager.shouldRetry(task));
        manager.scheduleRetry(task);

        assertEquals(TaskLifecycle.QUEUED, task.getLifecycle());
        verify(repository).save(task);
    }

    @Test
    void shouldNotScheduleRetryWhenAttemptsAreExhausted() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-r2", "file-r2", "ocr", "{}", TaskLifecycle.RUNNING, "worker-1", 2, 1, null, null);

        RetryManager manager = new RetryManager(repository, mock(SchedulerTaskAssignmentService.class), 1L);

        assertFalse(manager.shouldRetry(task));
        manager.scheduleRetry(task);

        assertEquals(TaskLifecycle.FAILED, task.getLifecycle());
        verify(repository).save(task);
    }

    @Test
    void shouldCalculateExponentialBackoffValues() {
        RetryManager manager = new RetryManager(mock(TaskRepository.class), mock(SchedulerTaskAssignmentService.class), 1L);
        assertEquals(100L, manager.calculateDelayMs(1));
        assertEquals(200L, manager.calculateDelayMs(2));
        assertEquals(400L, manager.calculateDelayMs(3));
    }

    @Test
    void shouldKeepTaskQueuedWhenRetryIsTriggered() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-r4", "file-r4", "ocr", "{}", TaskLifecycle.QUEUED, null, 1, 2, null, null);
        when(repository.findByTaskId(task.getTaskId())).thenReturn(Optional.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerTaskAssignmentService assignmentService = mock(SchedulerTaskAssignmentService.class);
        RetryManager manager = new RetryManager(repository, assignmentService, 1L);

        manager.triggerScheduledRetry(task.getTaskId());

        assertEquals(TaskLifecycle.QUEUED, task.getLifecycle());
        verify(assignmentService).assignTask(task.getTaskId());
    }

    @Test
    void shouldOnlyScheduleOneRetryForDuplicateFailure() throws Exception {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-r3", "file-r3", "ocr", "{}", TaskLifecycle.RUNNING, "worker-1", 1, 2, null, null);
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerTaskAssignmentService assignmentService = mock(SchedulerTaskAssignmentService.class);
        RetryManager manager = new RetryManager(repository, assignmentService, 1L);

        manager.scheduleRetry(task);
        manager.scheduleRetry(task);

        assertEquals(1, manager.getScheduledRetryCount(task.getTaskId()));
        assertEquals(TaskLifecycle.QUEUED, task.getLifecycle());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> manager.triggerScheduledRetry(task.getTaskId()));
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }
}
