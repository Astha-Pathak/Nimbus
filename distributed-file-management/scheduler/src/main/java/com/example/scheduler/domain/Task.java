package com.example.scheduler.domain;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "tasks", uniqueConstraints = {
        @UniqueConstraint(name = "uk_tasks_request_id", columnNames = "request_id")
}, indexes = {
        @Index(name = "idx_tasks_task_id", columnList = "task_id", unique = true),
        @Index(name = "idx_tasks_status", columnList = "lifecycle")
})
public class Task {

    @Id
    @Column(name = "task_id", nullable = false, updatable = false, length = 36)
    private String taskId;

    @Column(name = "request_id", nullable = false, length = 128)
    private String requestId;

    @Column(name = "file_id", nullable = false, length = 255)
    private String fileId;

    @Column(name = "processor_type", nullable = false, length = 128)
    private String processorType;

    @Lob
    @Column(name = "configuration", nullable = false, columnDefinition = "TEXT")
    private String configuration;

    @Enumerated(EnumType.STRING)
    @Column(name = "lifecycle", nullable = false, length = 32)
    private TaskLifecycle lifecycle;

    @Column(name = "assigned_worker_id", length = 128)
    private String assignedWorkerId;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

    @Column(name = "error_code", length = 128)
    private String errorCode;

    @Lob
    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Task() {
    }

    public Task(String requestId, String fileId, String processorType,
            String configuration, TaskLifecycle lifecycle,
            String assignedWorkerId, int attemptCount, int maxRetries,
            String errorCode, String errorMessage) {
        this.taskId = UUID.randomUUID().toString();
        this.requestId = requestId;
        this.fileId = fileId;
        this.processorType = processorType;
        this.configuration = configuration;
        this.lifecycle = lifecycle;
        this.assignedWorkerId = assignedWorkerId;
        this.attemptCount = attemptCount;
        this.maxRetries = maxRetries;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    @PrePersist
    public void beforePersist() {
        if (taskId == null || taskId.isBlank()) {
            taskId = UUID.randomUUID().toString();
        }
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (updatedAt == null) {
            updatedAt = Instant.now();
        }
    }

    @PreUpdate
    public void beforeUpdate() {
        updatedAt = Instant.now();
    }

    public String getTaskId() {
        return taskId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(String configuration) {
        this.configuration = configuration;
    }

    public TaskLifecycle getLifecycle() {
        return lifecycle;
    }

    public void setLifecycle(TaskLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    public String getAssignedWorkerId() {
        return assignedWorkerId;
    }

    public void setAssignedWorkerId(String assignedWorkerId) {
        this.assignedWorkerId = assignedWorkerId;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
