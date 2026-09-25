package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerHeartbeatRequest;
import com.example.nimbus.v1.WorkerHeartbeatResponse;
import com.example.nimbus.v1.WorkerRegisterRequest;
import com.example.nimbus.v1.WorkerRegisterResponse;
import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.InMemoryWorkerRegistry;
import com.example.scheduler.registry.WorkerRegistry;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class WorkerServiceImplTest {

    @Test
    void shouldRegisterValidWorker() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        WorkerServiceImpl service = new WorkerServiceImpl(registry);
        StreamObserver<WorkerRegisterResponse> responseObserver = mock(StreamObserver.class);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("worker-1")
                        .addSupportedProcessorTypes("ocr")
                        .setCapacity(4)
                        .setHost("worker-1.internal")
                        .build(),
                responseObserver);

        verify(responseObserver).onNext(any(WorkerRegisterResponse.class));
        verify(responseObserver).onCompleted();

        WorkerRegisterResponse response = responseCaptor(responseObserver);
        assertTrue(response.getAccepted());
        assertEquals("worker-1", response.getWorkerId());
        assertEquals(WorkerState.AVAILABLE, response.getState());

        WorkerInfo stored = registry.findById("worker-1").orElseThrow();
        assertEquals(WorkerState.AVAILABLE, stored.state());
        assertEquals("worker-1.internal", stored.host());
    }

    @Test
    void shouldRejectBlankWorkerId() {
        WorkerServiceImpl service = new WorkerServiceImpl(new InMemoryWorkerRegistry());
        StreamObserver<WorkerRegisterResponse> responseObserver = mock(StreamObserver.class);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("   ")
                        .addSupportedProcessorTypes("ocr")
                        .setCapacity(2)
                        .setHost("host")
                        .build(),
                responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectInvalidCapacity() {
        WorkerServiceImpl service = new WorkerServiceImpl(new InMemoryWorkerRegistry());
        StreamObserver<WorkerRegisterResponse> responseObserver = mock(StreamObserver.class);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("worker-2")
                        .addSupportedProcessorTypes("ocr")
                        .setCapacity(0)
                        .setHost("host")
                        .build(),
                responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectBlankHost() {
        WorkerServiceImpl service = new WorkerServiceImpl(new InMemoryWorkerRegistry());
        StreamObserver<WorkerRegisterResponse> responseObserver = mock(StreamObserver.class);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("worker-3")
                        .addSupportedProcessorTypes("ocr")
                        .setCapacity(2)
                        .setHost("   ")
                        .build(),
                responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectEmptySupportedProcessorTypes() {
        WorkerServiceImpl service = new WorkerServiceImpl(new InMemoryWorkerRegistry());
        StreamObserver<WorkerRegisterResponse> responseObserver = mock(StreamObserver.class);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("worker-4")
                        .setCapacity(2)
                        .setHost("host")
                        .build(),
                responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldReplaceExistingWorkerOnReRegistration() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        WorkerServiceImpl service = new WorkerServiceImpl(registry);
        StreamObserver<WorkerRegisterResponse> responseObserver = mock(StreamObserver.class);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("worker-5")
                        .addSupportedProcessorTypes("ocr")
                        .setCapacity(2)
                        .setHost("old-host")
                        .build(),
                responseObserver);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("worker-5")
                        .addSupportedProcessorTypes("ocr")
                        .addSupportedProcessorTypes("resize")
                        .setCapacity(5)
                        .setHost("new-host")
                        .build(),
                responseObserver);

        assertEquals(1, registry.listAvailable().size());
        WorkerInfo updated = registry.findById("worker-5").orElseThrow();
        assertEquals("new-host", updated.host());
        assertEquals(5, updated.capacity());
        assertEquals(List.of("ocr", "resize"), updated.supportedProcessorTypes());
        assertEquals(WorkerState.AVAILABLE, updated.state());
    }

    @Test
    void shouldAllowConcurrentRegistrationsWithoutCorruption() throws Exception {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        WorkerServiceImpl service = new WorkerServiceImpl(registry);
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch latch = new CountDownLatch(1);

        try {
            for (int i = 0; i < 8; i++) {
                final int index = i;
                executor.submit(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }

                    service.registerWorker(WorkerRegisterRequest.newBuilder()
                                    .setWorkerId("worker-" + (index % 3))
                                    .addSupportedProcessorTypes("ocr")
                                    .setCapacity(2)
                                    .setHost("host-" + index)
                                    .build(),
                            mock(StreamObserver.class));
                });
            }

            latch.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(3, registry.listAvailable().size());
            assertTrue(registry.findById("worker-0").isPresent());
            assertTrue(registry.findById("worker-1").isPresent());
            assertTrue(registry.findById("worker-2").isPresent());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldAcceptKnownWorkerHeartbeatAndUpdateStateAndTaskId() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        WorkerServiceImpl service = new WorkerServiceImpl(registry);
        StreamObserver<WorkerHeartbeatResponse> responseObserver = mock(StreamObserver.class);

        service.registerWorker(WorkerRegisterRequest.newBuilder()
                        .setWorkerId("worker-heartbeat")
                        .addSupportedProcessorTypes("ocr")
                        .setCapacity(2)
                        .setHost("host-heartbeat")
                        .build(),
                mock(StreamObserver.class));

        service.heartbeat(WorkerHeartbeatRequest.newBuilder()
                        .setWorkerId("worker-heartbeat")
                        .setStatus(WorkerState.BUSY)
                        .setCurrentTaskId("task-42")
                        .build(),
                responseObserver);

        verify(responseObserver).onNext(argThat(response -> response.getAccepted()));
        verify(responseObserver).onCompleted();

        WorkerInfo updated = registry.findById("worker-heartbeat").orElseThrow();
        assertEquals(WorkerState.BUSY, updated.state());
        assertEquals("task-42", updated.currentTaskId());
        assertNotNull(updated.lastHeartbeatAt());
    }

    @Test
    void shouldRejectUnknownWorkerHeartbeat() {
        WorkerServiceImpl service = new WorkerServiceImpl(new InMemoryWorkerRegistry());
        StreamObserver<WorkerHeartbeatResponse> responseObserver = mock(StreamObserver.class);

        service.heartbeat(WorkerHeartbeatRequest.newBuilder()
                        .setWorkerId("missing-worker")
                        .setStatus(WorkerState.AVAILABLE)
                        .setCurrentTaskId("task-99")
                        .build(),
                responseObserver);

        verify(responseObserver).onNext(argThat(response -> !response.getAccepted()));
        verify(responseObserver).onCompleted();
    }

    private static WorkerRegisterResponse responseCaptor(StreamObserver<WorkerRegisterResponse> responseObserver) {
        @SuppressWarnings("unchecked")
        var captor = (org.mockito.ArgumentCaptor<WorkerRegisterResponse>) org.mockito.ArgumentCaptor.forClass(WorkerRegisterResponse.class);
        verify(responseObserver).onNext(captor.capture());
        return captor.getValue();
    }
}
