package com.example.gateway;

import com.example.common.storage.FileStorage;
import com.example.nimbus.v1.FileChunk;
import com.example.nimbus.v1.FileMetadata;
import com.example.nimbus.v1.GatewayDownloadFileRequest;
import com.example.nimbus.v1.GatewayDownloadFileResponse;
import com.example.nimbus.v1.GatewayUploadFileRequest;
import com.example.nimbus.v1.GatewayUploadFileResponse;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GatewayServiceImplTest {

    @Test
    void shouldUploadFileSuccessfully() throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        when(fileStorage.store(any(InputStream.class))).thenReturn("file-123");

        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setFilename("hello.txt")
                        .setSize(5)
                        .setContentType("text/plain")
                        .build())
                .build());
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder().setChunkIndex(0)
                        .setData(ByteString.copyFrom("hello".getBytes(StandardCharsets.UTF_8))).build())
                .build());
        requestObserver.onCompleted();

        ArgumentCaptor<GatewayUploadFileResponse> responseCaptor = ArgumentCaptor
                .forClass(GatewayUploadFileResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        GatewayUploadFileResponse response = responseCaptor.getValue();
        assertEquals("file-123", response.getFileId());
        assertEquals("hello.txt", response.getFilename());
        assertEquals(5, response.getSize());
    }

    @Test
    void shouldDownloadFileSuccessfully() throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        String fileId = "file-123";
        byte[] payload = "hello world".getBytes(StandardCharsets.UTF_8);
        when(fileStorage.metadata(fileId)).thenReturn(Optional.of(new com.example.common.storage.FileMetadata(
                fileId,
                "hello.txt",
                payload.length,
                "text/plain",
                Instant.now())));
        when(fileStorage.open(fileId)).thenReturn(new ByteArrayInputStream(payload));

        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayDownloadFileResponse> responseObserver = mock(StreamObserver.class);

        service.downloadFile(GatewayDownloadFileRequest.newBuilder().setFileId(fileId).build(), responseObserver);

        ArgumentCaptor<GatewayDownloadFileResponse> responseCaptor = ArgumentCaptor
                .forClass(GatewayDownloadFileResponse.class);
        verify(responseObserver, atLeastOnce()).onNext(responseCaptor.capture());
        List<GatewayDownloadFileResponse> responses = responseCaptor.getAllValues();

        assertFalse(responses.isEmpty());
        assertTrue(responses.get(0).hasMetadata());
        assertEquals("hello.txt", responses.get(0).getMetadata().getFilename());
        assertEquals("text/plain", responses.get(0).getMetadata().getContentType());

        ByteArrayOutputStream reconstructed = new ByteArrayOutputStream();
        for (int i = 1; i < responses.size(); i++) {
            GatewayDownloadFileResponse response = responses.get(i);
            assertTrue(response.hasChunk());
            assertEquals(i - 1, response.getChunk().getChunkIndex());
            reconstructed.write(response.getChunk().getData().toByteArray());
        }

        assertEquals(new String(payload, StandardCharsets.UTF_8), reconstructed.toString(StandardCharsets.UTF_8));
        verify(responseObserver).onCompleted();
    }

    @Test
    void shouldRejectBlankDownloadFileId() {
        FileStorage fileStorage = mock(FileStorage.class);
        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayDownloadFileResponse> responseObserver = mock(StreamObserver.class);

        service.downloadFile(GatewayDownloadFileRequest.newBuilder().setFileId("   ").build(), responseObserver);

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectMissingFileForDownload() throws Exception {
        String fileId = "missing-file";
        FileStorage fileStorage = mock(FileStorage.class);
        when(fileStorage.metadata(fileId)).thenReturn(Optional.empty());

        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayDownloadFileResponse> responseObserver = mock(StreamObserver.class);

        service.downloadFile(GatewayDownloadFileRequest.newBuilder().setFileId(fileId).build(), responseObserver);

        verify(responseObserver).onError(argThat(exception -> exception instanceof StatusRuntimeException
                && ((StatusRuntimeException) exception).getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND));
    }

    @Test
    void shouldHandleStorageReadFailureDuringDownload() throws Exception {
        String fileId = "failing-file";
        FileStorage fileStorage = mock(FileStorage.class);
        when(fileStorage.metadata(fileId)).thenReturn(Optional.of(new com.example.common.storage.FileMetadata(
                fileId,
                "bad.txt",
                10L,
                "text/plain",
                Instant.now())));
        when(fileStorage.open(fileId)).thenThrow(new IOException("read failed"));

        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayDownloadFileResponse> responseObserver = mock(StreamObserver.class);

        service.downloadFile(GatewayDownloadFileRequest.newBuilder().setFileId(fileId).build(), responseObserver);

        verify(responseObserver).onError(argThat(exception -> exception instanceof StatusRuntimeException
                && ((StatusRuntimeException) exception).getStatus().getCode() == io.grpc.Status.Code.INTERNAL));
    }

    @Test
    void shouldDownloadEmptyFileSuccessfully() throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        String fileId = "empty-file";
        when(fileStorage.metadata(fileId)).thenReturn(Optional.of(new com.example.common.storage.FileMetadata(
                fileId,
                "empty.txt",
                0L,
                "text/plain",
                Instant.now())));
        when(fileStorage.open(fileId)).thenReturn(new ByteArrayInputStream(new byte[0]));

        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayDownloadFileResponse> responseObserver = mock(StreamObserver.class);

        service.downloadFile(GatewayDownloadFileRequest.newBuilder().setFileId(fileId).build(), responseObserver);

        ArgumentCaptor<GatewayDownloadFileResponse> responseCaptor = ArgumentCaptor
                .forClass(GatewayDownloadFileResponse.class);
        verify(responseObserver, atLeastOnce()).onNext(responseCaptor.capture());
        List<GatewayDownloadFileResponse> responses = responseCaptor.getAllValues();
        assertEquals(1, responses.size());
        assertTrue(responses.get(0).hasMetadata());
        assertEquals("empty.txt", responses.get(0).getMetadata().getFilename());
        verify(responseObserver).onCompleted();
    }

    @Test
    void shouldRejectMetadataNotFirst() {
        FileStorage fileStorage = mock(FileStorage.class);
        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder().setChunkIndex(0)
                        .setData(ByteString.copyFrom("hello".getBytes(StandardCharsets.UTF_8))).build())
                .build());

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectMissingMetadata() {
        FileStorage fileStorage = mock(FileStorage.class);
        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onCompleted();

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectBlankFilename() {
        FileStorage fileStorage = mock(FileStorage.class);
        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder().setFilename("   ").setSize(1).build())
                .build());

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectInvalidDeclaredSize() {
        FileStorage fileStorage = mock(FileStorage.class);
        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder().setFilename("file.txt").setSize(-1).build())
                .build());

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectIncorrectFinalByteCount() throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        when(fileStorage.store(any(InputStream.class))).thenAnswer(invocation -> {
            InputStream in = invocation.getArgument(0);
            byte[] bytes = in.readAllBytes();
            assertEquals("hello", new String(bytes, StandardCharsets.UTF_8));
            return "file-123";
        });

        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder().setFilename("file.txt").setSize(10).build())
                .build());
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder().setChunkIndex(0)
                        .setData(ByteString.copyFrom("hello".getBytes(StandardCharsets.UTF_8))).build())
                .build());
        requestObserver.onCompleted();

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectInvalidChunkOrdering() {
        FileStorage fileStorage = mock(FileStorage.class);
        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder().setFilename("file.txt").setSize(5).build())
                .build());
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder().setChunkIndex(1)
                        .setData(ByteString.copyFrom("hello".getBytes(StandardCharsets.UTF_8))).build())
                .build());

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldRejectEmptyUpload() {
        FileStorage fileStorage = mock(FileStorage.class);
        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder().setFilename("empty.txt").setSize(0).build())
                .build());
        requestObserver.onCompleted();

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    void shouldHandleFileStorageFailure() throws Exception {
        FileStorage fileStorage = mock(FileStorage.class);
        when(fileStorage.store(any(InputStream.class))).thenThrow(new IOException("disk full"));

        GatewayServiceImpl service = new GatewayServiceImpl(fileStorage);
        StreamObserver<GatewayUploadFileResponse> responseObserver = mock(StreamObserver.class);

        StreamObserver<GatewayUploadFileRequest> requestObserver = service.uploadFile(responseObserver);
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder().setFilename("file.txt").setSize(5).build())
                .build());
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder().setChunkIndex(0)
                        .setData(ByteString.copyFrom("hello".getBytes(StandardCharsets.UTF_8))).build())
                .build());
        requestObserver.onCompleted();

        verify(responseObserver).onError(any(StatusRuntimeException.class));
    }
}
