package com.example.common.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;

public interface FileStorage {

    String store(InputStream inputStream) throws IOException;

    InputStream open(String fileId) throws IOException;

    boolean exists(String fileId);

    void delete(String fileId) throws IOException;

    Optional<FileMetadata> metadata(String fileId) throws IOException;

    Path getRootDirectory();
}
