package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerServiceGrpc;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.scheduler.domain.WorkerInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;

@Service
public class WorkerGrpcClient implements WorkerClient {

    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;

    public WorkerGrpcClient() {
        this("localhost", 9090);
    }

    public WorkerGrpcClient(String host, int port) {
        String targetHost = host == null || host.isBlank() ? "localhost" : host.trim();
        this.channel = ManagedChannelBuilder.forAddress(targetHost, port)
                .usePlaintext()
                .build();
        this.blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public WorkerAssignTaskResponse assignTask(WorkerInfo worker, WorkerTaskAssignment assignment) {
        if (worker == null || worker.workerId() == null || worker.workerId().isBlank()) {
            throw new IllegalArgumentException("Worker ID must not be blank");
        }
        if (assignment == null) {
            throw new IllegalArgumentException("Assignment must not be null");
        }

        String target = worker.host() == null ? "localhost" : worker.host().trim();
        String host = target;
        int port = 9090;

        if (target.contains(":")) {
            int lastColonIndex = target.lastIndexOf(':');
            host = target.substring(0, lastColonIndex);
            port = Integer.parseInt(target.substring(lastColonIndex + 1));
        }

        ManagedChannel assignmentChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            WorkerServiceGrpc.WorkerServiceBlockingStub assignmentStub =
                    WorkerServiceGrpc.newBlockingStub(assignmentChannel);
            return assignmentStub.assignTask(assignment);
        } finally {
            assignmentChannel.shutdown();
        }
    }

    @PreDestroy
    public void shutdown() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
    }
}
