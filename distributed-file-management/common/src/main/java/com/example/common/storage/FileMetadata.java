package com.example.common.storage;

import java.time.Instant;

public record FileMetadata(
        String fileId,
        String fileName,
        long size,
        String contentType,
        Instant createdAt) {
}
