package com.example.worker.processor;

import com.example.common.storage.FileStorage;
import com.example.common.storage.LocalFileStorage;
import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.TaskResultData;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChecksumProcessor implements TaskProcessor {

    private static final int BUFFER_SIZE = 8192;

    private final FileStorage fileStorage;

    public ChecksumProcessor() {
        this(new LocalFileStorage("./data/files"));
    }

    public ChecksumProcessor(FileStorage fileStorage) {
        this.fileStorage = fileStorage;
    }

    @Override
    public String processorType() {
        return "CHECKSUM";
    }

    @Override
    public TaskResultData process(String fileId, TaskConfiguration configuration) {
        if (fileId == null || fileId.isBlank()) {
            throw new IllegalArgumentException("File ID must not be blank");
        }
        if (!fileStorage.exists(fileId)) {
            throw new IllegalArgumentException("File not found: " + fileId);
        }

        try (InputStream inputStream = fileStorage.open(fileId)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }

            String checksum = toHex(digest.digest());
            return TaskResultData.newBuilder()
                    .putValues("checksum", checksum)
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read file for checksum: " + fileId, e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available in this runtime", e);
        }
    }

    private static String toHex(byte[] digest) {
        StringBuilder builder = new StringBuilder(digest.length * 2);
        for (byte value : digest) {
            builder.append(String.format("%02x", value));
        }
        return builder.toString();
    }
}
