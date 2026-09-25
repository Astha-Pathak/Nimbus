package com.example.scheduler.service;

import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerState;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.InMemoryWorkerRegistry;
import com.example.scheduler.repository.TaskRepository;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SchedulerTaskAssignmentServiceTest {

    @Test
    void shouldAssignQueuedTaskToEligibleWorker() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-1", "file-1", "ocr", "{}", TaskLifecycle.QUEUED,
                null, 0, 3, null, null);
        when(repository.findByTaskId(task.getTaskId())).thenReturn(Optional.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo("worker-1", List.of("ocr"), 2, "localhost:9091",
                WorkerState.AVAILABLE, Instant.now(), Instant.now()));

        WorkerClient workerClient = mock(WorkerClient.class);
        when(workerClient.assignTask(any(WorkerInfo.class), any(WorkerTaskAssignment.class)))
                .thenReturn(WorkerAssignTaskResponse.newBuilder().setAccepted(true).build());

        SchedulerTaskAssignmentService service = new SchedulerTaskAssignmentService(
                repository, registry, new RoundRobinSchedulingStrategy(), workerClient);

        Optional<Task> assigned = service.assignTask(task.getTaskId());

        assertTrue(assigned.isPresent());
        assertEquals("worker-1", assigned.get().getAssignedWorkerId());
        verify(workerClient).assignTask(any(WorkerInfo.class), any(WorkerTaskAssignment.class));
        verify(repository).save(task);
    }

    @Test
    void shouldRejectTaskWhenNoEligibleWorkerExists() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-2", "file-2", "resize", "{}", TaskLifecycle.QUEUED,
                null, 0, 3, null, null);
        when(repository.findByTaskId(task.getTaskId())).thenReturn(Optional.of(task));

        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo("worker-1", List.of("ocr"), 2, "localhost:9091",
                WorkerState.AVAILABLE, Instant.now(), Instant.now()));

        WorkerClient workerClient = mock(WorkerClient.class);
        SchedulerTaskAssignmentService service = new SchedulerTaskAssignmentService(
                repository, registry, new RoundRobinSchedulingStrategy(), workerClient);

        Optional<Task> assigned = service.assignTask(task.getTaskId());

        assertTrue(assigned.isEmpty());
        assertNull(task.getAssignedWorkerId());
        verify(workerClient, never()).assignTask(any(), any());
    }

    @Test
    void shouldNotAssignTaskWhenWorkerRejectsAssignment() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-3", "file-3", "ocr", "{}", TaskLifecycle.QUEUED,
                null, 0, 3, null, null);
        when(repository.findByTaskId(task.getTaskId())).thenReturn(Optional.of(task));

        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo("worker-1", List.of("ocr"), 2, "localhost:9091",
                WorkerState.AVAILABLE, Instant.now(), Instant.now()));

        WorkerClient workerClient = mock(WorkerClient.class);
        when(workerClient.assignTask(any(WorkerInfo.class), any(WorkerTaskAssignment.class)))
                .thenReturn(WorkerAssignTaskResponse.newBuilder().setAccepted(false).build());

        SchedulerTaskAssignmentService service = new SchedulerTaskAssignmentService(
                repository, registry, new RoundRobinSchedulingStrategy(), workerClient);

        Optional<Task> assigned = service.assignTask(task.getTaskId());

        assertTrue(assigned.isEmpty());
        assertNull(task.getAssignedWorkerId());
        verify(repository, never()).save(any(Task.class));
    }

    @Test
    void shouldBuildAssignmentRequestWithTaskMetadata() {
        TaskRepository repository = mock(TaskRepository.class);
        Task task = new Task("req-4", "file-4", "ocr", "{\"mode\":\"fast\"}", TaskLifecycle.QUEUED,
                null, 0, 3, null, null);
        when(repository.findByTaskId(task.getTaskId())).thenReturn(Optional.of(task));
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        InMemoryWorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo("worker-2", List.of("ocr"), 2, "localhost:9091",
                WorkerState.AVAILABLE, Instant.now(), Instant.now()));

        WorkerClient workerClient = mock(WorkerClient.class);
        when(workerClient.assignTask(any(WorkerInfo.class), any(WorkerTaskAssignment.class)))
                .thenReturn(WorkerAssignTaskResponse.newBuilder().setAccepted(true).build());

        SchedulerTaskAssignmentService service = new SchedulerTaskAssignmentService(
                repository, registry, new RoundRobinSchedulingStrategy(), workerClient);

        service.assignTask(task.getTaskId());

        verify(workerClient).assignTask(any(WorkerInfo.class), argThat(assignment ->
                assignment.getTaskId().equals(task.getTaskId())
                        && assignment.getFileId().equals("file-4")
                        && assignment.getProcessorType().equals("ocr")
                        && assignment.getAttempt() == 1
                        && assignment.getMaxRetries() == 3
                        && assignment.hasConfiguration()));
        assertEquals(1, task.getAttemptCount());
    }
}
