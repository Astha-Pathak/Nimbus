package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerRegisterRequest;
import com.example.nimbus.v1.WorkerRegisterResponse;
import com.example.nimbus.v1.WorkerServiceGrpc;
import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.WorkerRegistry;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@GrpcService
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final WorkerRegistry workerRegistry;

    public WorkerServiceImpl(WorkerRegistry workerRegistry) {
        this.workerRegistry = workerRegistry;
    }

    @Override
    public void registerWorker(WorkerRegisterRequest request,
            StreamObserver<WorkerRegisterResponse> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must not be null")
                    .asRuntimeException());
            return;
        }

        String workerId = request.getWorkerId();
        if (workerId == null || workerId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Worker ID must not be blank")
                    .asRuntimeException());
            return;
        }

        int capacity = request.getCapacity();
        if (capacity <= 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Capacity must be greater than zero")
                    .asRuntimeException());
            return;
        }

        List<String> supportedProcessorTypes = request.getSupportedProcessorTypesList().stream()
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
        if (supportedProcessorTypes.isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("At least one supported processor type is required")
                    .asRuntimeException());
            return;
        }

        String host = request.getHost();
        if (host == null || host.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Host must not be blank")
                    .asRuntimeException());
            return;
        }

        Instant now = Instant.now();
        WorkerInfo workerInfo = new WorkerInfo(
                workerId.trim(),
                new ArrayList<>(supportedProcessorTypes),
                capacity,
                host.trim(),
                WorkerState.AVAILABLE,
                now,
                now);

        workerRegistry.register(workerInfo);

        responseObserver.onNext(WorkerRegisterResponse.newBuilder()
                .setWorkerId(workerInfo.workerId())
                .setAccepted(true)
                .setState(WorkerState.AVAILABLE)
                .build());
        responseObserver.onCompleted();
    }
}
