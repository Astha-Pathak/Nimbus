package com.example.common.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LocalFileStorageTest {

    @TempDir
    Path tempDir;

    private LocalFileStorage storage;

    @BeforeEach
    void setUp() {
        storage = new LocalFileStorage(tempDir.toString());
    }

    @Test
    void shouldStoreFileSuccessfully() throws IOException {
        String content = "hello from storage";

        String fileId = storage.store(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

        assertNotNull(fileId);
        assertFalse(fileId.isBlank());
        assertTrue(storage.exists(fileId));
        assertEquals(content, new String(storage.open(fileId).readAllBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void shouldGenerateUniqueFileIds() throws IOException {
        String first = storage.store(new ByteArrayInputStream("first".getBytes(StandardCharsets.UTF_8)));
        String second = storage.store(new ByteArrayInputStream("second".getBytes(StandardCharsets.UTF_8)));

        assertNotEquals(first, second);
        assertTrue(storage.exists(first));
        assertTrue(storage.exists(second));
    }

    @Test
    void shouldReadStoredContentBack() throws IOException {
        byte[] payload = "payload-bytes".getBytes(StandardCharsets.UTF_8);

        String fileId = storage.store(new ByteArrayInputStream(payload));
        try (InputStream inputStream = storage.open(fileId)) {
            assertArrayEquals(payload, inputStream.readAllBytes());
        }
    }

    @Test
    void shouldReportMissingFile() {
        assertFalse(storage.exists("missing-file-id"));
    }

    @Test
    void shouldThrowForMissingFileOnOpen() {
        assertThrows(IOException.class, () -> storage.open("missing-file-id"));
    }

    @Test
    void shouldCreateStorageDirectoryAutomatically() throws IOException {
        Path rootDirectory = tempDir.resolve("nested").resolve("files");
        LocalFileStorage nestedStorage = new LocalFileStorage(rootDirectory.toString());

        nestedStorage.store(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));

        assertTrue(Files.isDirectory(rootDirectory));
    }
}
