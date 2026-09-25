package com.example.worker.processor;

import com.example.common.storage.FileStorage;
import com.example.common.storage.LocalFileStorage;
import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.TaskResultData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompressionProcessorTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCompressKnownTextSuccessfully() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        CompressionProcessor processor = new CompressionProcessor(storage);
        String content = "The quick brown fox jumps over the lazy dog.";
        String fileId = storage.store(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());

        assertNotNull(result);
        assertTrue(result.getValuesMap().containsKey("output_file_id"));
        assertNotEquals(fileId, result.getValuesMap().get("output_file_id"));

        String outputFileId = result.getValuesMap().get("output_file_id");
        assertTrue(storage.exists(outputFileId));
        assertEquals(String.valueOf(content.getBytes(StandardCharsets.UTF_8).length), result.getValuesMap().get("original_size"));
        assertTrue(Long.parseLong(result.getValuesMap().get("compressed_size")) > 0);
        assertTrue(Double.parseDouble(result.getValuesMap().get("compression_ratio")) > 0.0);

        try (InputStream inputStream = storage.open(outputFileId);
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
            byte[] decompressed = gzipInputStream.readAllBytes();
            assertEquals(content, new String(decompressed, StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldHandleEmptyFileWithoutDivisionByZero() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        CompressionProcessor processor = new CompressionProcessor(storage);
        String fileId = storage.store(new ByteArrayInputStream(new byte[0]));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());

        assertEquals("0", result.getValuesMap().get("original_size"));
        assertEquals("0", result.getValuesMap().get("compression_ratio"));
    }

    @Test
    void shouldPreserveBinaryDataAfterCompression() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        CompressionProcessor processor = new CompressionProcessor(storage);
        byte[] payload = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, (byte) 255, (byte) 128, (byte) 200};
        String fileId = storage.store(new ByteArrayInputStream(payload));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());

        String outputFileId = result.getValuesMap().get("output_file_id");
        try (InputStream inputStream = storage.open(outputFileId);
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
            assertArrayEquals(payload, gzipInputStream.readAllBytes());
        }
    }

    @Test
    void shouldUseReturnedOutputFileIdForRetrieval() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        CompressionProcessor processor = new CompressionProcessor(storage);
        String fileId = storage.store(new ByteArrayInputStream("sample-data".getBytes(StandardCharsets.UTF_8)));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());
        String outputFileId = result.getValuesMap().get("output_file_id");

        assertTrue(storage.exists(outputFileId));
        assertNotEquals(fileId, outputFileId);
        try (InputStream inputStream = storage.open(outputFileId);
                GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
            assertEquals("sample-data", new String(gzipInputStream.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void shouldLeaveOriginalFileUnchanged() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        CompressionProcessor processor = new CompressionProcessor(storage);
        byte[] original = "stable-content".getBytes(StandardCharsets.UTF_8);
        String fileId = storage.store(new ByteArrayInputStream(original));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());

        try (InputStream inputStream = storage.open(fileId)) {
            assertArrayEquals(original, inputStream.readAllBytes());
        }
        assertEquals(String.valueOf(original.length), result.getValuesMap().get("original_size"));
    }

    @Test
    void shouldFailForMissingSourceFile() {
        FileStorage storage = mock(FileStorage.class);
        when(storage.exists("missing-file")).thenReturn(false);
        CompressionProcessor processor = new CompressionProcessor(storage);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> processor.process("missing-file", TaskConfiguration.getDefaultInstance()));

        assertTrue(exception.getMessage().contains("File not found"));
    }

    @Test
    void shouldFailWhenOutputWriteFails() throws IOException {
        FileStorage storage = mock(FileStorage.class);
        when(storage.exists("source-file")).thenReturn(true);
        when(storage.metadata("source-file")).thenReturn(java.util.Optional.of(new com.example.common.storage.FileMetadata("source-file", "source.txt", 10, "text/plain", java.time.Instant.now())));
        when(storage.open("source-file")).thenReturn(new ByteArrayInputStream("abcde".getBytes(StandardCharsets.UTF_8)));
        when(storage.createFile()).thenThrow(new IOException("output write failed"));
        CompressionProcessor processor = new CompressionProcessor(storage);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> processor.process("source-file", TaskConfiguration.getDefaultInstance()));

        assertTrue(exception.getMessage().contains("Unable to create output file"));
    }
}
