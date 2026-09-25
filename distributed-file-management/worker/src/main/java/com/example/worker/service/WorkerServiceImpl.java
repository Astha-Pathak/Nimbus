package com.example.worker.service;

import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.nimbus.v1.WorkerServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@GrpcService
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final Map<String, WorkerTaskAssignment> assignedTasks = new ConcurrentHashMap<>();

    public Map<String, WorkerTaskAssignment> getAssignedTasks() {
        return assignedTasks;
    }

    @Override
    public void assignTask(WorkerTaskAssignment request,
            StreamObserver<WorkerAssignTaskResponse> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must not be null")
                    .asRuntimeException());
            return;
        }

        String taskId = request.getTaskId();
        String fileId = request.getFileId();
        String processorType = request.getProcessorType();
        int attempt = request.getAttempt();

        if (taskId == null || taskId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Task ID must not be blank")
                    .asRuntimeException());
            return;
        }
        if (fileId == null || fileId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("File ID must not be blank")
                    .asRuntimeException());
            return;
        }
        if (processorType == null || processorType.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Processor type must not be blank")
                    .asRuntimeException());
            return;
        }
        if (attempt < 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Attempt must be greater than or equal to zero")
                    .asRuntimeException());
            return;
        }

        WorkerTaskAssignment existing = assignedTasks.putIfAbsent(taskId.trim(), request);
        if (existing != null) {
            responseObserver.onNext(WorkerAssignTaskResponse.newBuilder()
                    .setAccepted(true)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        responseObserver.onNext(WorkerAssignTaskResponse.newBuilder()
                .setAccepted(true)
                .build());
        responseObserver.onCompleted();
    }
}
