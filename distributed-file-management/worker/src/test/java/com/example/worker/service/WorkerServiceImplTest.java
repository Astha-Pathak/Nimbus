package com.example.worker.service;

import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.TaskResultData;
import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.worker.processor.ProcessorRegistry;
import com.example.worker.processor.TaskProcessor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class WorkerServiceImplTest {

    @Test
    void shouldAcceptValidChecksumAssignmentAndExecuteProcessor() {
        AtomicBoolean invoked = new AtomicBoolean(false);
        TaskProcessor processor = (fileId, configuration) -> {
            invoked.set(true);
            return TaskResultData.newBuilder()
                    .putValues("checksum", "abc123")
                    .build();
        };

        WorkerServiceImpl service = new WorkerServiceImpl(new ProcessorRegistry(Map.of("CHECKSUM", processor)));
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-1")
                        .setFileId("file-1")
                        .setProcessorType("CHECKSUM")
                        .setAttempt(0)
                        .setMaxRetries(3)
                        .build(),
                observer);

        verify(observer).onNext(any(WorkerAssignTaskResponse.class));
        verify(observer).onCompleted();
        assertTrue(invoked.get());
        assertTrue(service.getAssignedTasks().containsKey("task-1"));
        assertTrue(service.getExecutionResults().containsKey("task-1"));
        assertEquals("abc123", service.getExecutionResults().get("task-1").getResult().getValuesMap().get("checksum"));
    }

    @Test
    void shouldRejectInvalidAssignment() {
        WorkerServiceImpl service = new WorkerServiceImpl();
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("   ")
                        .setFileId("file-1")
                        .setProcessorType("CHECKSUM")
                        .setAttempt(0)
                        .build(),
                observer);

        verify(observer).onError(any(StatusRuntimeException.class));
        assertFalse(service.getAssignedTasks().containsKey("task-1"));
    }

    @Test
    void shouldRejectInvalidAttemptValue() {
        WorkerServiceImpl service = new WorkerServiceImpl();
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-2")
                        .setFileId("file-2")
                        .setProcessorType("CHECKSUM")
                        .setAttempt(-1)
                        .build(),
                observer);

        verify(observer).onError(any(StatusRuntimeException.class));
        assertFalse(service.getAssignedTasks().containsKey("task-2"));
    }

    @Test
    void shouldPreventDuplicateAssignments() {
        WorkerServiceImpl service = new WorkerServiceImpl();
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-3")
                        .setFileId("file-3")
                        .setProcessorType("CHECKSUM")
                        .setAttempt(0)
                        .build(),
                observer);

        StreamObserver<WorkerAssignTaskResponse> secondObserver = mock(StreamObserver.class);
        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-3")
                        .setFileId("file-3")
                        .setProcessorType("CHECKSUM")
                        .setAttempt(1)
                        .build(),
                secondObserver);

        verify(secondObserver).onNext(argThat(response -> response.getAccepted()));
        verify(secondObserver).onCompleted();
        assertEquals(1, service.getAssignedTasks().size());
    }

    @Test
    void shouldRejectUnsupportedProcessorType() {
        WorkerServiceImpl service = new WorkerServiceImpl();
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-unsupported")
                        .setFileId("file-unsupported")
                        .setProcessorType("COMPRESSION")
                        .setAttempt(0)
                        .build(),
                observer);

        verify(observer).onError(any(StatusRuntimeException.class));
        assertFalse(service.getAssignedTasks().containsKey("task-unsupported"));
    }

    @Test
    void shouldCaptureProcessorFailureAsExecutionError() {
        TaskProcessor failingProcessor = (fileId, configuration) -> {
            throw new IllegalStateException("checksum failed");
        };
        WorkerServiceImpl service = new WorkerServiceImpl(new ProcessorRegistry(Map.of("CHECKSUM", failingProcessor)));
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-fail")
                        .setFileId("file-fail")
                        .setProcessorType("CHECKSUM")
                        .setAttempt(0)
                        .build(),
                observer);

        verify(observer).onNext(any(WorkerAssignTaskResponse.class));
        verify(observer).onCompleted();
        assertTrue(service.getExecutionResults().containsKey("task-fail"));
        assertFalse(service.getExecutionResults().get("task-fail").getError().getMessage().isBlank());
    }
}
