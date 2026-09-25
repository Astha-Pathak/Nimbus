package com.example.worker.processor;

import com.example.common.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ProcessorRegistryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldResolveChecksumProcessor() throws IOException {
        LocalFileStorage storage = new LocalFileStorage(tempDir.toString());
        ProcessorRegistry registry = new ProcessorRegistry();
        registry.register("CHECKSUM", new ChecksumProcessor(storage));

        assertTrue(registry.supports("CHECKSUM"));
        assertTrue(registry.resolve("checksum").isPresent());
        assertEquals("CHECKSUM", registry.resolve("checksum").orElseThrow().processorType());
    }

    @Test
    void shouldRejectUnsupportedProcessorType() {
        ProcessorRegistry registry = new ProcessorRegistry();

        assertFalse(registry.supports("COMPRESSION"));
        assertTrue(registry.resolve("COMPRESSION").isEmpty());
    }
}
