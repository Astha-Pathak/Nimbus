package com.example.demo;

import com.example.nimbus.v1.GatewayGetTaskStatusResponse;
import com.example.nimbus.v1.GatewaySubmitTaskResponse;
import com.example.nimbus.v1.TaskLifecycle;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class NimbusDemo {

    private static final String GATEWAY_HOST = System.getenv().getOrDefault("GATEWAY_HOST", "localhost");
    private static final int GATEWAY_PORT = Integer.parseInt(System.getenv().getOrDefault("GATEWAY_PORT", "8080"));
    private static final int POLL_TIMEOUT_SECONDS = 60;
    private static final int POLL_INTERVAL_MS = 500;

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("NIMBUS DISTRIBUTED FILE SYSTEM DEMO");
        System.out.println("========================================");

        try (NimbusDemoClient client = new NimbusDemoClient(GATEWAY_HOST, GATEWAY_PORT)) {

            // Scenario 2: Upload files
            System.out.println("\n[2/7] Uploading 10 test files...");
            List<String> fileIds = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                String filename = String.format("file-%02d.txt", i);
                String content = String.format("Nimbus demo file %d - checksum and compression payload data. UUID=%s", i, UUID.randomUUID());
                byte[] data = content.getBytes(StandardCharsets.UTF_8);
                String fileId = client.uploadFile(filename, data);
                fileIds.add(fileId);
                System.out.printf("  Uploaded %s -> fileId=%s (%d bytes)%n", filename, fileId, data.length);
            }

            // Scenario 3: Submit CHECKSUM tasks
            System.out.println("\n[3/7] Submitting 10 CHECKSUM tasks...");
            List<String> taskIds = new ArrayList<>();
            for (int i = 0; i < fileIds.size(); i++) {
                String fileId = fileIds.get(i);
                GatewaySubmitTaskResponse response = client.submitTask(fileId, "CHECKSUM");
                taskIds.add(response.getTaskId());
                System.out.printf("  Task %d: taskId=%s status=%s%n", i + 1, response.getTaskId(), response.getStatus());
            }

            // Poll for completion
            System.out.println("\n  Polling task status...");
            for (int i = 0; i < taskIds.size(); i++) {
                String taskId = taskIds.get(i);
                GatewayGetTaskStatusResponse status = pollUntilComplete(client, taskId);
                String result = status.hasResult() ? status.getResult().getValuesMap().get("checksum") : "N/A";
                System.out.printf("  Task %d: %s -> COMPLETED worker=%s attempts=%d checksum=%s...%n",
                        i + 1, taskId, status.getWorkerId(), status.getAttemptCount(),
                        result != null && result.length() > 16 ? result.substring(0, 16) : result);
            }

            // Scenario 5: COMPRESSION
            System.out.println("\n[5/7] Submitting COMPRESSION task...");
            GatewaySubmitTaskResponse compressResponse = client.submitTask(fileIds.get(0), "COMPRESSION");
            System.out.printf("  Compression task: taskId=%s status=%s%n", compressResponse.getTaskId(), compressResponse.getStatus());
            GatewayGetTaskStatusResponse compressStatus = pollUntilComplete(client, compressResponse.getTaskId());
            String outputFileId = compressStatus.hasResult() ? compressStatus.getResult().getValuesMap().get("output_file_id") : "N/A";
            System.out.printf("  Compression: %s -> COMPLETED worker=%s outputFileId=%s%n",
                    compressResponse.getTaskId(), compressStatus.getWorkerId(), outputFileId);

            System.out.println("\n========================================");
            System.out.println("DEMO COMPLETE");
            System.out.println("========================================");
            System.out.println("All tasks completed successfully.");
        }
    }

    private static GatewayGetTaskStatusResponse pollUntilComplete(NimbusDemoClient client, String taskId) throws Exception {
        long deadline = System.currentTimeMillis() + (POLL_TIMEOUT_SECONDS * 1000L);
        TaskLifecycle lastStatus = null;
        while (System.currentTimeMillis() < deadline) {
            GatewayGetTaskStatusResponse status = client.getTaskStatus(taskId);
            lastStatus = status.getStatus();
            if (lastStatus == TaskLifecycle.COMPLETED || lastStatus == TaskLifecycle.FAILED) {
                return status;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        throw new RuntimeException("Task " + taskId + " did not complete within " + POLL_TIMEOUT_SECONDS + " seconds. Last status: " + lastStatus);
    }
}