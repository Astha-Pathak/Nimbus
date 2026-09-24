package com.example.common.storage;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class LocalFileStorage implements FileStorage {

    private final Path rootDirectory;

    public LocalFileStorage(@Value("${file-storage.root-directory:./data/files}") String rootDirectory) {
        this.rootDirectory = normalizeRootDirectory(rootDirectory);
        createStorageDirectoryIfNeeded();
    }

    @Override
    public String store(InputStream inputStream) throws IOException {
        if (inputStream == null) {
            throw new IllegalArgumentException("InputStream must not be null");
        }

        String fileId = UUID.randomUUID().toString();
        Path targetPath = resolvePath(fileId);

        Files.createDirectories(targetPath.getParent());

        try (InputStream in = new BufferedInputStream(inputStream);
                OutputStream out = new BufferedOutputStream(
                        Files.newOutputStream(targetPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
        }

        return fileId;
    }

    @Override
    public InputStream open(String fileId) throws IOException {
        Path filePath = resolvePath(fileId);
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + fileId);
        }
        return Files.newInputStream(filePath);
    }

    @Override
    public boolean exists(String fileId) {
        try {
            return Files.exists(resolvePath(fileId));
        } catch (Exception ignored) {
            return false;
        }
    }

    @Override
    public void delete(String fileId) throws IOException {
        Path filePath = resolvePath(fileId);
        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }
    }

    @Override
    public Optional<FileMetadata> metadata(String fileId) throws IOException {
        Path filePath = resolvePath(fileId);
        if (!Files.exists(filePath)) {
            return Optional.empty();
        }

        return Optional.of(new FileMetadata(
                fileId,
                filePath.getFileName().toString(),
                Files.size(filePath),
                Files.probeContentType(filePath),
                Instant.ofEpochMilli(Files.getLastModifiedTime(filePath).toMillis())));
    }

    @Override
    public Path getRootDirectory() {
        return rootDirectory;
    }

    private void createStorageDirectoryIfNeeded() {
        try {
            Files.createDirectories(rootDirectory);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create file storage directory: " + rootDirectory, e);
        }
    }

    private Path resolvePath(String fileId) {
        if (fileId == null || fileId.isBlank()) {
            throw new IllegalArgumentException("File ID must not be null or blank");
        }

        Path filePath = rootDirectory.resolve(fileId);
        Path normalizedRoot = rootDirectory.toAbsolutePath().normalize();
        Path normalizedFile = filePath.toAbsolutePath().normalize();

        if (!normalizedFile.startsWith(normalizedRoot)) {
            throw new SecurityException("Invalid file path: " + fileId);
        }

        return normalizedFile;
    }

    private static Path normalizeRootDirectory(String rootDirectory) {
        if (rootDirectory == null || rootDirectory.isBlank()) {
            throw new IllegalArgumentException("File storage root directory must not be blank");
        }

        return Paths.get(rootDirectory).toAbsolutePath().normalize();
    }
}
