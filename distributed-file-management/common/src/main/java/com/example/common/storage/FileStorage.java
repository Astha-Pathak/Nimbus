package com.example.common.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Optional;

public interface FileStorage {

    String store(InputStream inputStream) throws IOException;

    default String createFile() throws IOException {
        return store(new java.io.ByteArrayInputStream(new byte[0]));
    }

    InputStream open(String fileId) throws IOException;

    default OutputStream openForWrite(String fileId) throws IOException {
        return new java.io.BufferedOutputStream(java.nio.file.Files.newOutputStream(
                java.nio.file.Path.of(getRootDirectory().toString(), fileId),
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.TRUNCATE_EXISTING,
                java.nio.file.StandardOpenOption.WRITE));
    }

    boolean exists(String fileId);

    void delete(String fileId) throws IOException;

    Optional<FileMetadata> metadata(String fileId) throws IOException;

    Path getRootDirectory();
}
