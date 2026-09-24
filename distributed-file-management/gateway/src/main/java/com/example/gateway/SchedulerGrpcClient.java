package com.example.gateway;

import com.example.nimbus.v1.SchedulerSubmitTaskRequest;
import com.example.nimbus.v1.SchedulerSubmitTaskResponse;
import com.example.nimbus.v1.SchedulerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SchedulerGrpcClient implements SchedulerClient {

    private final ManagedChannel channel;
    private final SchedulerServiceGrpc.SchedulerServiceBlockingStub blockingStub;

    public SchedulerGrpcClient(
            @Value("${scheduler.grpc.host:localhost}") String host,
            @Value("${scheduler.grpc.port:9090}") int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = SchedulerServiceGrpc.newBlockingStub(channel);
    }

    public SchedulerSubmitTaskResponse submitTask(SchedulerSubmitTaskRequest request) {
        return blockingStub.submitTask(request);
    }

    @PreDestroy
    public void shutdown() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
    }
}
