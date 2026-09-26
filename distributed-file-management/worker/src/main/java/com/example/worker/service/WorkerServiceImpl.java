package com.example.worker.service;

import com.example.nimbus.v1.TaskError;
import com.example.nimbus.v1.TaskResultData;
import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerHeartbeatRequest;
import com.example.nimbus.v1.WorkerHeartbeatResponse;
import com.example.nimbus.v1.WorkerRegisterRequest;
import com.example.nimbus.v1.WorkerRegisterResponse;
import com.example.nimbus.v1.WorkerServiceGrpc;
import com.example.nimbus.v1.WorkerState;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.worker.processor.ChecksumProcessor;
import com.example.worker.processor.CompressionProcessor;
import com.example.worker.processor.ProcessorRegistry;
import com.example.worker.processor.TaskProcessor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@GrpcService
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final String DEFAULT_SCHEDULER_HOST = "localhost";
    private static final int DEFAULT_SCHEDULER_PORT = 9090;

    private final Map<String, WorkerTaskAssignment> assignedTasks = new ConcurrentHashMap<>();
    private final Map<String, TaskExecutionResult> executionResults = new ConcurrentHashMap<>();
    private final ProcessorRegistry processorRegistry;
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
    private final String workerId;
    private final String host;
    private final String schedulerHost;
    private final int schedulerPort;
    private final long heartbeatIntervalMs;
    private volatile String currentTaskId;

    public WorkerServiceImpl() {
        this(defaultProcessorRegistry(), null, null, DEFAULT_SCHEDULER_HOST, DEFAULT_SCHEDULER_PORT, 5000L);
    }

    public WorkerServiceImpl(ProcessorRegistry processorRegistry) {
        this(processorRegistry, null, null, DEFAULT_SCHEDULER_HOST, DEFAULT_SCHEDULER_PORT, 5000L);
    }

    public WorkerServiceImpl(ProcessorRegistry processorRegistry, String workerId, String host, String schedulerHost, int schedulerPort, long heartbeatIntervalMs) {
        this.processorRegistry = processorRegistry == null ? defaultProcessorRegistry() : processorRegistry;
        this.workerId = workerId == null || workerId.isBlank() ? "worker-local" : workerId.trim();
        this.host = host == null || host.isBlank() ? "localhost" : host.trim();
        this.schedulerHost = schedulerHost == null || schedulerHost.isBlank() ? DEFAULT_SCHEDULER_HOST : schedulerHost.trim();
        this.schedulerPort = schedulerPort <= 0 ? DEFAULT_SCHEDULER_PORT : schedulerPort;
        this.heartbeatIntervalMs = heartbeatIntervalMs > 0 ? heartbeatIntervalMs : 5000L;
        startHeartbeatLoop();
    }

    public void shutdown() {
        heartbeatScheduler.shutdownNow();
    }

    public Map<String, WorkerTaskAssignment> getAssignedTasks() {
        return assignedTasks;
    }

    public Map<String, TaskExecutionResult> getExecutionResults() {
        return executionResults;
    }

    public String getCurrentTaskId() {
        return currentTaskId;
    }

    public void setCurrentTaskId(String currentTaskId) {
        this.currentTaskId = currentTaskId == null || currentTaskId.isBlank() ? null : currentTaskId.trim();
    }

    @Override
    public void registerWorker(WorkerRegisterRequest request,
            StreamObserver<WorkerRegisterResponse> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must not be null")
                    .asRuntimeException());
            return;
        }

        responseObserver.onNext(WorkerRegisterResponse.newBuilder()
                .setWorkerId(request.getWorkerId())
                .setAccepted(true)
                .setState(WorkerState.AVAILABLE)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(WorkerHeartbeatRequest request,
            StreamObserver<WorkerHeartbeatResponse> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must not be null")
                    .asRuntimeException());
            return;
        }

        responseObserver.onNext(WorkerHeartbeatResponse.newBuilder()
                .setAccepted(true)
                .build());
        responseObserver.onCompleted();
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

        setCurrentTaskId(taskId.trim());
        TaskExecutionResult executionResult = executeTask(taskId.trim(), fileId.trim(), normalizedProcessorType, request.getConfiguration());
        executionResults.put(taskId.trim(), executionResult);
        setCurrentTaskId(null);

        responseObserver.onNext(WorkerAssignTaskResponse.newBuilder()
                .setAccepted(true)
                .build());
        responseObserver.onCompleted();
    }

    private void startHeartbeatLoop() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                ManagedChannel channel = ManagedChannelBuilder.forAddress(schedulerHost, schedulerPort)
                        .usePlaintext()
                        .build();
                try {
                    WorkerServiceGrpc.WorkerServiceBlockingStub stub = WorkerServiceGrpc.newBlockingStub(channel);
                    WorkerHeartbeatRequest request = WorkerHeartbeatRequest.newBuilder()
                            .setWorkerId(workerId)
                            .setStatus(WorkerState.AVAILABLE)
                            .setCurrentTaskId(currentTaskId == null ? "" : currentTaskId)
                            .build();
                    stub.heartbeat(request);
                } finally {
                    channel.shutdown();
                }
            } catch (Exception ignored) {
                // Heartbeat failures are intentionally ignored for this step.
            }
        }, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
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
        registry.register("COMPRESSION", new CompressionProcessor());
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
