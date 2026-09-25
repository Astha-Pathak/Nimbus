package com.example.worker.processor;

import com.example.common.storage.FileStorage;
import com.example.common.storage.LocalFileStorage;
import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.TaskResultData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionProcessor implements TaskProcessor {

    private final FileStorage fileStorage;

    public CompressionProcessor() {
        this(new LocalFileStorage("./data/files"));
    }

    public CompressionProcessor(FileStorage fileStorage) {
        this.fileStorage = fileStorage;
    }

    @Override
    public String processorType() {
        return "COMPRESSION";
    }

    @Override
    public TaskResultData process(String fileId, TaskConfiguration configuration) {
        if (fileId == null || fileId.isBlank()) {
            throw new IllegalArgumentException("File ID must not be blank");
        }
        if (!fileStorage.exists(fileId)) {
            throw new IllegalArgumentException("File not found: " + fileId);
        }

        long originalSize;
        try {
            originalSize = fileStorage.metadata(fileId)
                    .orElseThrow(() -> new IllegalArgumentException("File not found: " + fileId))
                    .size();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read file metadata: " + fileId, e);
        }

        String outputFileId;
        try {
            outputFileId = fileStorage.createFile();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create output file for compression: " + fileId, e);
        }

        try (InputStream inputStream = fileStorage.open(fileId);
                OutputStream outputStream = fileStorage.openForWrite(outputFileId);
                GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                gzipOutputStream.write(buffer, 0, bytesRead);
            }
            gzipOutputStream.finish();
        } catch (IOException e) {
            try {
                fileStorage.delete(outputFileId);
            } catch (IOException ignored) {
                // best effort cleanup
            }
            throw new IllegalStateException("Unable to compress file: " + fileId, e);
        }

        try {
            long compressedSize = fileStorage.metadata(outputFileId)
                    .orElseThrow(() -> new IllegalStateException("Compressed output was not created: " + outputFileId))
                    .size();
            String compressionRatio = originalSize == 0 ? "0" : String.valueOf(((double) compressedSize / originalSize));

            return TaskResultData.newBuilder()
                    .putValues("output_file_id", outputFileId)
                    .putValues("original_size", String.valueOf(originalSize))
                    .putValues("compressed_size", String.valueOf(compressedSize))
                    .putValues("compression_ratio", compressionRatio)
                    .build();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to inspect compressed output: " + outputFileId, e);
        }
    }
}
