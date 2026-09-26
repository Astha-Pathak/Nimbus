package com.example.demo;

import com.example.nimbus.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class NimbusDemoClient implements AutoCloseable {

    private final ManagedChannel channel;
private final GatewayServiceGrpc.GatewayServiceBlockingStub blockingStub;
    private final GatewayServiceGrpc.GatewayServiceStub asyncStub;

    public NimbusDemoClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = GatewayServiceGrpc.newBlockingStub(channel);
        this.asyncStub = GatewayServiceGrpc.newStub(channel);
    }

    public String uploadFile(String filename, byte[] data) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<GatewayUploadFileResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<GatewayUploadFileRequest> requestObserver = asyncStub.uploadFile(
                new StreamObserver<>() {
                    @Override
                    public void onNext(GatewayUploadFileResponse value) {
                        responseRef.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        errorRef.set(t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                });

        GatewayUploadFileRequest metadata = GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setFilename(filename)
                        .setSize(data.length)
                        .setContentType("text/plain")
                        .build())
                .build();
        requestObserver.onNext(metadata);

        GatewayUploadFileRequest chunkMsg = GatewayUploadFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder()
                        .setChunkIndex(0)
                        .setData(com.google.protobuf.ByteString.copyFrom(data))
                        .build())
                .build();
        requestObserver.onNext(chunkMsg);
        requestObserver.onCompleted();

        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new RuntimeException("Upload timed out");
        }
        if (errorRef.get() != null) {
            throw new RuntimeException("Upload failed", errorRef.get());
        }
        if (responseRef.get() == null) {
            throw new RuntimeException("Upload returned no response");
        }
        return responseRef.get().getFileId();
    }

    public GatewaySubmitTaskResponse submitTask(String fileId, String processorType) {
        return blockingStub.submitTask(GatewaySubmitTaskRequest.newBuilder()
                .setFileId(fileId)
                .setProcessorType(processorType)
                .build());
    }

    public GatewayGetTaskStatusResponse getTaskStatus(String taskId) {
        return blockingStub.getTaskStatus(GatewayGetTaskStatusRequest.newBuilder()
                .setTaskId(taskId)
                .build());
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}