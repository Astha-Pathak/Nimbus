package com.example.worker.processor;

import com.example.common.storage.FileStorage;
import com.example.common.storage.LocalFileStorage;
import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.TaskResultData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ChecksumProcessorTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldComputeKnownTextChecksum() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        ChecksumProcessor processor = new ChecksumProcessor(storage);

        String fileId = storage.store(new ByteArrayInputStream("hello world".getBytes(StandardCharsets.UTF_8)));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());

        assertEquals("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
                result.getValuesMap().get("checksum"));
    }

    @Test
    void shouldComputeEmptyFileChecksum() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        ChecksumProcessor processor = new ChecksumProcessor(storage);

        String fileId = storage.store(new ByteArrayInputStream(new byte[0]));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());

        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                result.getValuesMap().get("checksum"));
    }

    @Test
    void shouldComputeBinaryFileChecksum() throws IOException, java.security.NoSuchAlgorithmException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        ChecksumProcessor processor = new ChecksumProcessor(storage);
        byte[] payload = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, (byte) 255, (byte) 128, (byte) 200};

        String fileId = storage.store(new ByteArrayInputStream(payload));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload);
        String expected = HexFormat.of().formatHex(digest);

        assertEquals(expected, result.getValuesMap().get("checksum"));
    }

    @Test
    void shouldUseChecksumResultKey() throws IOException {
        FileStorage storage = new LocalFileStorage(tempDir.toString());
        ChecksumProcessor processor = new ChecksumProcessor(storage);
        String fileId = storage.store(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)));

        TaskResultData result = processor.process(fileId, TaskConfiguration.getDefaultInstance());

        assertTrue(result.getValuesMap().containsKey("checksum"));
        assertFalse(result.getValuesMap().get("checksum").isBlank());
        assertTrue(result.getValuesMap().get("checksum").matches("[0-9a-f]+"));
    }

    @Test
    void shouldFailWhenFileDoesNotExist() {
        FileStorage storage = mock(FileStorage.class);
        when(storage.exists("missing-file")).thenReturn(false);
        ChecksumProcessor processor = new ChecksumProcessor(storage);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> processor.process("missing-file", TaskConfiguration.getDefaultInstance()));

        assertTrue(exception.getMessage().contains("File not found"));
    }

    @Test
    void shouldFailWhenFileCannotBeRead() throws IOException {
        FileStorage storage = mock(FileStorage.class);
        when(storage.exists("broken-file")).thenReturn(true);
        when(storage.open("broken-file")).thenThrow(new IOException("disk error"));
        ChecksumProcessor processor = new ChecksumProcessor(storage);

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> processor.process("broken-file", TaskConfiguration.getDefaultInstance()));

        assertTrue(exception.getMessage().contains("Unable to read file"));
    }
}
