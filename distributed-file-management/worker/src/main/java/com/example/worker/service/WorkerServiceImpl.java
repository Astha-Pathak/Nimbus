package com.example.worker.service;

import com.example.nimbus.v1.TaskError;
import com.example.nimbus.v1.TaskResultData;
import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.nimbus.v1.WorkerServiceGrpc;
import com.example.worker.processor.ChecksumProcessor;
import com.example.worker.processor.ProcessorRegistry;
import com.example.worker.processor.TaskProcessor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@GrpcService
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final Map<String, WorkerTaskAssignment> assignedTasks = new ConcurrentHashMap<>();
    private final Map<String, TaskExecutionResult> executionResults = new ConcurrentHashMap<>();
    private final ProcessorRegistry processorRegistry;

    public WorkerServiceImpl() {
        this(defaultProcessorRegistry());
    }

    public WorkerServiceImpl(ProcessorRegistry processorRegistry) {
        this.processorRegistry = processorRegistry == null ? defaultProcessorRegistry() : processorRegistry;
    }

    public Map<String, WorkerTaskAssignment> getAssignedTasks() {
        return assignedTasks;
    }

    public Map<String, TaskExecutionResult> getExecutionResults() {
        return executionResults;
    }

    @Override
    public void assignTask(WorkerTaskAssignment request,
            StreamObserver<WorkerAssignTaskResponse> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must not be null")
                    .asRuntimeException());
            return;
        }

        String taskId = request.getTaskId();
        String fileId = request.getFileId();
        String processorType = request.getProcessorType();
        int attempt = request.getAttempt();

        if (taskId == null || taskId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Task ID must not be blank")
                    .asRuntimeException());
            return;
        }
        if (fileId == null || fileId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("File ID must not be blank")
                    .asRuntimeException());
            return;
        }
        if (processorType == null || processorType.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Processor type must not be blank")
                    .asRuntimeException());
            return;
        }
        if (attempt < 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Attempt must be greater than or equal to zero")
                    .asRuntimeException());
            return;
        }

        String normalizedProcessorType = ProcessorRegistry.normalize(processorType);
        if (!processorRegistry.supports(normalizedProcessorType)) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Unsupported processor type: " + processorType)
                    .asRuntimeException());
            return;
        }

        WorkerTaskAssignment existing = assignedTasks.putIfAbsent(taskId.trim(), request);
        if (existing != null) {
            responseObserver.onNext(WorkerAssignTaskResponse.newBuilder()
                    .setAccepted(true)
                    .build());
            responseObserver.onCompleted();
            return;
        }

        TaskExecutionResult executionResult = executeTask(taskId.trim(), fileId.trim(), normalizedProcessorType, request.getConfiguration());
        executionResults.put(taskId.trim(), executionResult);

        responseObserver.onNext(WorkerAssignTaskResponse.newBuilder()
                .setAccepted(true)
                .build());
        responseObserver.onCompleted();
    }

    private TaskExecutionResult executeTask(String taskId, String fileId, String processorType, com.example.nimbus.v1.TaskConfiguration configuration) {
        Optional<TaskProcessor> processor = processorRegistry.resolve(processorType);
        if (processor.isEmpty()) {
            return TaskExecutionResult.failure(taskId, fileId, processorType, "Unsupported processor type: " + processorType);
        }

        try {
            TaskResultData result = processor.get().process(fileId, configuration);
            return TaskExecutionResult.success(taskId, fileId, processorType, result);
        } catch (Exception e) {
            return TaskExecutionResult.failure(taskId, fileId, processorType, e.getMessage() == null ? "Execution failed" : e.getMessage());
        }
    }

    private static ProcessorRegistry defaultProcessorRegistry() {
        ProcessorRegistry registry = new ProcessorRegistry();
        registry.register("CHECKSUM", new ChecksumProcessor());
        return registry;
    }

    public static final class TaskExecutionResult {
        private final String taskId;
        private final String fileId;
        private final String processorType;
        private final TaskResultData result;
        private final TaskError error;

        private TaskExecutionResult(String taskId, String fileId, String processorType, TaskResultData result, TaskError error) {
            this.taskId = taskId;
            this.fileId = fileId;
            this.processorType = processorType;
            this.result = result;
            this.error = error;
        }

        public static TaskExecutionResult success(String taskId, String fileId, String processorType, TaskResultData result) {
            return new TaskExecutionResult(taskId, fileId, processorType, result, null);
        }

        public static TaskExecutionResult failure(String taskId, String fileId, String processorType, String message) {
            TaskError error = TaskError.newBuilder()
                    .setCode("PROCESSOR_FAILURE")
                    .setMessage(message)
                    .build();
            return new TaskExecutionResult(taskId, fileId, processorType, null, error);
        }

        public String getTaskId() {
            return taskId;
        }

        public String getFileId() {
            return fileId;
        }

        public String getProcessorType() {
            return processorType;
        }

        public TaskResultData getResult() {
            return result;
        }

        public TaskError getError() {
            return error;
        }
    }
}
