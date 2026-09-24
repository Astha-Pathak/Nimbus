package com.example.gateway;

import com.example.common.storage.FileStorage;
import com.example.nimbus.v1.FileChunk;
import com.example.nimbus.v1.FileMetadata;
import com.example.nimbus.v1.GatewayDownloadFileRequest;
import com.example.nimbus.v1.GatewayDownloadFileResponse;
import com.example.nimbus.v1.GatewayServiceGrpc;
import com.example.nimbus.v1.GatewayUploadFileRequest;
import com.example.nimbus.v1.GatewayUploadFileResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@GrpcService
public class GatewayServiceImpl extends GatewayServiceGrpc.GatewayServiceImplBase {

    private final FileStorage fileStorage;
    private final int chunkSizeBytes;

    public GatewayServiceImpl(FileStorage fileStorage) {
        this(fileStorage, 64 * 1024);
    }

    public GatewayServiceImpl(FileStorage fileStorage,
            @Value("${gateway.file-transfer.chunk-size:65536}") int chunkSizeBytes) {
        this.fileStorage = fileStorage;
        this.chunkSizeBytes = Math.max(1, chunkSizeBytes);
    }

    @Override
    public void downloadFile(GatewayDownloadFileRequest request,
            StreamObserver<GatewayDownloadFileResponse> responseObserver) {
        if (request == null || request.getFileId() == null || request.getFileId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("File ID must not be blank")
                    .asRuntimeException());
            return;
        }

        String fileId = request.getFileId().trim();

        try {
            Optional<com.example.common.storage.FileMetadata> metadata = fileStorage.metadata(fileId);
            if (metadata.isEmpty()) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("File not found: " + fileId)
                        .asRuntimeException());
                return;
            }

            com.example.common.storage.FileMetadata storedMetadata = metadata.get();
            responseObserver.onNext(GatewayDownloadFileResponse.newBuilder()
                    .setMetadata(FileMetadata.newBuilder()
                            .setFilename(storedMetadata.fileName())
                            .setSize(storedMetadata.size())
                            .setContentType(storedMetadata.contentType() == null ? "" : storedMetadata.contentType())
                            .build())
                    .build());

            try (InputStream inputStream = fileStorage.open(fileId)) {
                byte[] buffer = new byte[chunkSizeBytes];
                int bytesRead;
                int chunkIndex = 0;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    responseObserver.onNext(GatewayDownloadFileResponse.newBuilder()
                            .setChunk(FileChunk.newBuilder()
                                    .setChunkIndex(chunkIndex++)
                                    .setData(ByteString.copyFrom(buffer, 0, bytesRead)))
                            .build());
                }
            } catch (IOException e) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Unable to read file data")
                        .withCause(e)
                        .asRuntimeException());
                return;
            }

            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Unable to load file metadata")
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public StreamObserver<GatewayUploadFileRequest> uploadFile(
            StreamObserver<GatewayUploadFileResponse> responseObserver) {
        return new StreamObserver<>() {
            private final AtomicBoolean terminated = new AtomicBoolean(false);
            private final AtomicReference<FileMetadata> metadata = new AtomicReference<>();
            private final AtomicLong receivedBytes = new AtomicLong();
            private final AtomicLong lastChunkIndex = new AtomicLong(-1);
            private final AtomicReference<PipedOutputStream> outputStream = new AtomicReference<>();
            private final AtomicReference<Thread> storeThread = new AtomicReference<>();
            private final AtomicReference<String> fileId = new AtomicReference<>();
            private final AtomicReference<Throwable> storageFailure = new AtomicReference<>();

            @Override
            public void onNext(GatewayUploadFileRequest request) {
                if (terminated.get()) {
                    return;
                }

                if (request == null) {
                    fail(Status.INVALID_ARGUMENT.withDescription("Upload request must not be null"));
                    return;
                }

                if (metadata.get() == null) {
                    if (!request.hasMetadata()) {
                        fail(Status.INVALID_ARGUMENT.withDescription("The first message must contain file metadata"));
                        return;
                    }

                    FileMetadata fileMetadata = request.getMetadata();
                    if (fileMetadata.getFilename() == null || fileMetadata.getFilename().isBlank()) {
                        fail(Status.INVALID_ARGUMENT.withDescription("Filename must not be blank"));
                        return;
                    }
                    if (fileMetadata.getSize() < 0) {
                        fail(Status.INVALID_ARGUMENT.withDescription("Declared file size must be non-negative"));
                        return;
                    }

                    metadata.set(fileMetadata);

                    PipedInputStream inputStream = new PipedInputStream(8192);
                    PipedOutputStream pipedOutputStream = new PipedOutputStream();
                    try {
                        pipedOutputStream.connect(inputStream);
                    } catch (IOException e) {
                        fail(Status.INTERNAL.withDescription("Unable to initialize upload stream").withCause(e));
                        return;
                    }

                    outputStream.set(pipedOutputStream);

                    Thread worker = new Thread(() -> {
                        try {
                            fileId.set(fileStorage.store(inputStream));
                        } catch (IOException e) {
                            storageFailure.set(e);
                        } finally {
                            closeQuietly(inputStream);
                        }
                    }, "gateway-upload-store");
                    storeThread.set(worker);
                    worker.setDaemon(true);
                    worker.start();
                    return;
                }

                if (!request.hasChunk()) {
                    fail(Status.INVALID_ARGUMENT.withDescription("Only chunk messages are allowed after metadata"));
                    return;
                }

                FileChunk chunk = request.getChunk();
                long expectedIndex = lastChunkIndex.get() + 1;
                if (chunk.getChunkIndex() != expectedIndex) {
                    fail(Status.INVALID_ARGUMENT
                            .withDescription("Chunk indexes must start at 0 and increase sequentially"));
                    return;
                }

                byte[] chunkData = chunk.getData().toByteArray();
                PipedOutputStream currentOutput = outputStream.get();
                if (currentOutput == null) {
                    fail(Status.INTERNAL.withDescription("Upload stream is not initialized"));
                    return;
                }

                try {
                    currentOutput.write(chunkData);
                    lastChunkIndex.set(chunk.getChunkIndex());
                    receivedBytes.addAndGet(chunkData.length);
                } catch (IOException e) {
                    fail(Status.INTERNAL.withDescription("Failed to store upload data").withCause(e));
                }
            }

            @Override
            public void onError(Throwable t) {
                fail(Status.fromThrowable(t));
            }

            @Override
            public void onCompleted() {
                if (terminated.get()) {
                    return;
                }

                if (metadata.get() == null) {
                    fail(Status.INVALID_ARGUMENT.withDescription("Missing file metadata"));
                    return;
                }

                try {
                    PipedOutputStream currentOutput = outputStream.get();
                    if (currentOutput != null) {
                        currentOutput.close();
                    }
                    Thread worker = storeThread.get();
                    if (worker != null) {
                        worker.join();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail(Status.INTERNAL.withDescription("Upload interrupted").withCause(e));
                    return;
                } catch (IOException e) {
                    fail(Status.INTERNAL.withDescription("Failed to finalize upload").withCause(e));
                    return;
                }

                Throwable uploadFailure = storageFailure.get();
                if (uploadFailure != null) {
                    fail(Status.INTERNAL.withDescription("Failed to persist uploaded file").withCause(uploadFailure));
                    return;
                }

                long declaredSize = metadata.get().getSize();
                long actualSize = receivedBytes.get();
                if (declaredSize >= 0 && declaredSize != actualSize) {
                    cleanupStoredFile();
                    fail(Status.INVALID_ARGUMENT
                            .withDescription("Uploaded byte count does not match the declared file size"));
                    return;
                }

                String storedFileId = fileId.get();
                if (storedFileId == null || storedFileId.isBlank()) {
                    fail(Status.INTERNAL.withDescription("Upload did not produce a valid file identifier"));
                    return;
                }

                responseObserver.onNext(
                        GatewayUploadFileResponse.newBuilder()
                                .setFileId(storedFileId)
                                .setFilename(metadata.get().getFilename())
                                .setSize(actualSize)
                                .build());
                responseObserver.onCompleted();
                terminated.set(true);
            }

            private void fail(Status status) {
                if (!terminated.compareAndSet(false, true)) {
                    return;
                }

                try {
                    PipedOutputStream currentOutput = outputStream.get();
                    if (currentOutput != null) {
                        currentOutput.close();
                    }
                    Thread worker = storeThread.get();
                    if (worker != null) {
                        worker.join();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException ignored) {
                    // Ignore cleanup failures during error handling.
                }

                cleanupStoredFile();
                responseObserver.onError(status.asRuntimeException());
            }

            private void cleanupStoredFile() {
                String storedFileId = fileId.get();
                if (storedFileId == null || storedFileId.isBlank()) {
                    return;
                }
                try {
                    fileStorage.delete(storedFileId);
                } catch (IOException ignored) {
                    // Ignore cleanup failures during validation errors.
                }
            }
        };
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Ignore closure issues in background tasks.
        }
    }
}
