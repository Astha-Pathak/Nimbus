package com.example.scheduler.service;

import com.example.nimbus.v1.SchedulerSubmitTaskRequest;
import com.example.nimbus.v1.SchedulerSubmitTaskResponse;
import com.example.nimbus.v1.TaskConfiguration;
import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SchedulerServiceImplTest {

    @Test
    void shouldSubmitValidTaskAndPersistQueuedState() {
        TaskRepository repository = mock(TaskRepository.class);
        when(repository.findByRequestId("req-1")).thenReturn(Optional.empty());
        Task savedTask = new Task("req-1", "file-1", "ocr", "{\"mode\":\"fast\"}", TaskLifecycle.QUEUED,
                null, 0, 3, null, null);
        when(repository.save(any(Task.class))).thenAnswer(invocation -> {
            Task task = invocation.getArgument(0);
            return task;
        });

        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-1")
                        .setFileId("file-1")
                        .setProcessorType("ocr")
                        .setConfiguration(TaskConfiguration.newBuilder()
                                .putSettings("mode", "fast")
                                .build())
                        .build(),
                responseObserver);

        ArgumentCaptor<SchedulerSubmitTaskResponse> responseCaptor = ArgumentCaptor.forClass(SchedulerSubmitTaskResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        SchedulerSubmitTaskResponse response = responseCaptor.getValue();
        assertNotNull(response.getTaskId());
        assertEquals(com.example.nimbus.v1.TaskLifecycle.QUEUED, response.getStatus());
        assertEquals("req-1", response.getRequestId());

        verify(repository).save(any(Task.class));
    }

    @Test
    void shouldReturnPersistedTaskIdForDuplicateRequestId() {
        TaskRepository repository = mock(TaskRepository.class);
        Task existing = new Task("req-duplicate", "file-9", "ocr", "{}", TaskLifecycle.RUNNING,
                "worker-7", 1, 3, null, null);
        when(repository.findByRequestId("req-duplicate")).thenReturn(Optional.of(existing));

        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-duplicate")
                        .setFileId("file-9")
                        .setProcessorType("ocr")
                        .build(),
                responseObserver);

        ArgumentCaptor<SchedulerSubmitTaskResponse> captor = ArgumentCaptor.forClass(SchedulerSubmitTaskResponse.class);
        verify(responseObserver).onNext(captor.capture());
        verify(responseObserver).onCompleted();
        verify(repository, never()).save(any(Task.class));

        SchedulerSubmitTaskResponse response = captor.getValue();
        assertEquals(existing.getTaskId(), response.getTaskId());
        assertEquals(com.example.nimbus.v1.TaskLifecycle.RUNNING, response.getStatus());
        assertEquals("req-duplicate", response.getRequestId());
    }

    @Test
    void shouldRejectBlankRequestId() {
        TaskRepository repository = mock(TaskRepository.class);
        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("   ")
                        .setFileId("file-1")
                        .setProcessorType("ocr")
                        .build(),
                responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectBlankFileId() {
        TaskRepository repository = mock(TaskRepository.class);
        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-2")
                        .setFileId("   ")
                        .setProcessorType("ocr")
                        .build(),
                responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectBlankProcessorType() {
        TaskRepository repository = mock(TaskRepository.class);
        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-3")
                        .setFileId("file-1")
                        .setProcessorType("   ")
                        .build(),
                responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldPersistAttemptCountAtZero() {
        TaskRepository repository = mock(TaskRepository.class);
        when(repository.findByRequestId("req-4")).thenReturn(Optional.empty());
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-4")
                        .setFileId("file-4")
                        .setProcessorType("ocr")
                        .build(),
                responseObserver);

        ArgumentCaptor<Task> taskCaptor = ArgumentCaptor.forClass(Task.class);
        verify(repository).save(taskCaptor.capture());
        assertEquals(0, taskCaptor.getValue().getAttemptCount());
    }

    @Test
    void shouldUseConfiguredDefaultMaxRetries() {
        TaskRepository repository = mock(TaskRepository.class);
        when(repository.findByRequestId("req-5")).thenReturn(Optional.empty());
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 7);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-5")
                        .setFileId("file-5")
                        .setProcessorType("ocr")
                        .build(),
                responseObserver);

        ArgumentCaptor<Task> taskCaptor = ArgumentCaptor.forClass(Task.class);
        verify(repository).save(taskCaptor.capture());
        assertEquals(7, taskCaptor.getValue().getMaxRetries());
    }

    @Test
    void shouldPersistTaskConfiguration() {
        TaskRepository repository = mock(TaskRepository.class);
        when(repository.findByRequestId("req-6")).thenReturn(Optional.empty());
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-6")
                        .setFileId("file-6")
                        .setProcessorType("ocr")
                        .setConfiguration(TaskConfiguration.newBuilder()
                                .putSettings("mode", "fast")
                                .putSettings("priority", "high")
                                .build())
                        .build(),
                responseObserver);

        ArgumentCaptor<Task> taskCaptor = ArgumentCaptor.forClass(Task.class);
        verify(repository).save(taskCaptor.capture());
        Task savedTask = taskCaptor.getValue();
        assertTrue(savedTask.getConfiguration().contains("mode"));
        assertTrue(savedTask.getConfiguration().contains("priority"));
        assertTrue(savedTask.getConfiguration().contains("fast"));
        assertTrue(savedTask.getConfiguration().contains("high"));
    }

    @Test
    void shouldReturnQueuedStatusWithTaskIdAndRequestId() {
        TaskRepository repository = mock(TaskRepository.class);
        when(repository.findByRequestId("req-7")).thenReturn(Optional.empty());
        when(repository.save(any(Task.class))).thenAnswer(invocation -> invocation.getArgument(0));

        SchedulerServiceImpl service = new SchedulerServiceImpl(repository, 3);
        StreamObserver<SchedulerSubmitTaskResponse> responseObserver = mock(StreamObserver.class);

        service.submitTask(SchedulerSubmitTaskRequest.newBuilder()
                        .setRequestId("req-7")
                        .setFileId("file-7")
                        .setProcessorType("ocr")
                        .build(),
                responseObserver);

        ArgumentCaptor<SchedulerSubmitTaskResponse> responseCaptor = ArgumentCaptor.forClass(SchedulerSubmitTaskResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        SchedulerSubmitTaskResponse response = responseCaptor.getValue();
        assertFalse(response.getTaskId().isBlank());
        assertEquals(com.example.nimbus.v1.TaskLifecycle.QUEUED, response.getStatus());
        assertEquals("req-7", response.getRequestId());
    }
}
