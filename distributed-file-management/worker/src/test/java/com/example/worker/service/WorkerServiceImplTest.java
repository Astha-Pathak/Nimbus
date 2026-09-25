package com.example.worker.service;

import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerTaskAssignment;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class WorkerServiceImplTest {

    @Test
    void shouldAcceptValidAssignment() {
        WorkerServiceImpl service = new WorkerServiceImpl();
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-1")
                        .setFileId("file-1")
                        .setProcessorType("ocr")
                        .setAttempt(0)
                        .setMaxRetries(3)
                        .build(),
                observer);

        verify(observer).onNext(any(WorkerAssignTaskResponse.class));
        verify(observer).onCompleted();
        assertTrue(service.getAssignedTasks().containsKey("task-1"));
    }

    @Test
    void shouldRejectInvalidAssignment() {
        WorkerServiceImpl service = new WorkerServiceImpl();
        StreamObserver<WorkerAssignTaskResponse> observer = mock(StreamObserver.class);

        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("   ")
                        .setFileId("file-1")
                        .setProcessorType("ocr")
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
                        .setProcessorType("ocr")
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
                        .setProcessorType("ocr")
                        .setAttempt(0)
                        .build(),
                observer);

        StreamObserver<WorkerAssignTaskResponse> secondObserver = mock(StreamObserver.class);
        service.assignTask(WorkerTaskAssignment.newBuilder()
                        .setTaskId("task-3")
                        .setFileId("file-3")
                        .setProcessorType("ocr")
                        .setAttempt(1)
                        .build(),
                secondObserver);

        verify(secondObserver).onNext(argThat(response -> response.getAccepted()));
        verify(secondObserver).onCompleted();
        assertEquals(1, service.getAssignedTasks().size());
    }
}
