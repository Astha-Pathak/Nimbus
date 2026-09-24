package com.example.scheduler.service;

import com.example.nimbus.v1.SchedulerSubmitTaskRequest;
import com.example.nimbus.v1.SchedulerSubmitTaskResponse;
import com.example.nimbus.v1.SchedulerServiceGrpc;
import com.example.nimbus.v1.TaskConfiguration;
import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.repository.TaskRepository;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@GrpcService
public class SchedulerServiceImpl extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    private final TaskRepository taskRepository;
    private final int defaultMaxRetries;

    public SchedulerServiceImpl(TaskRepository taskRepository,
            @Value("${scheduler.task.default-max-retries:3}") int defaultMaxRetries) {
        this.taskRepository = taskRepository;
        this.defaultMaxRetries = Math.max(0, defaultMaxRetries);
    }

    @Override
    @Transactional
    public void submitTask(SchedulerSubmitTaskRequest request,
            StreamObserver<SchedulerSubmitTaskResponse> responseObserver) {
        if (request == null) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request must not be null")
                    .asRuntimeException());
            return;
        }

        String requestId = request.getRequestId();
        if (requestId == null || requestId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Request ID must not be blank")
                    .asRuntimeException());
            return;
        }

        String fileId = request.getFileId();
        if (fileId == null || fileId.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("File ID must not be blank")
                    .asRuntimeException());
            return;
        }

        String processorType = request.getProcessorType();
        if (processorType == null || processorType.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Processor type must not be blank")
                    .asRuntimeException());
            return;
        }

        String trimmedRequestId = requestId.trim();
        Optional<Task> existingTask = taskRepository.findByRequestId(trimmedRequestId);
        if (existingTask.isPresent()) {
            Task task = existingTask.get();
            responseObserver.onNext(SchedulerSubmitTaskResponse.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setStatus(toProtoLifecycle(task.getLifecycle()))
                    .setRequestId(task.getRequestId())
                    .build());
            responseObserver.onCompleted();
            return;
        }

        String serializedConfiguration = serializeConfiguration(request.getConfiguration());

        Task task = new Task(
                trimmedRequestId,
                fileId.trim(),
                processorType.trim(),
                serializedConfiguration,
                TaskLifecycle.QUEUED,
                null,
                0,
                defaultMaxRetries,
                null,
                null);

        Task savedTask = taskRepository.save(task);

        responseObserver.onNext(SchedulerSubmitTaskResponse.newBuilder()
                .setTaskId(savedTask.getTaskId())
                .setStatus(toProtoLifecycle(savedTask.getLifecycle()))
                .setRequestId(savedTask.getRequestId())
                .build());
        responseObserver.onCompleted();
    }

    private String serializeConfiguration(TaskConfiguration configuration) {
        if (configuration == null || configuration.getSettingsMap().isEmpty()) {
            return "{}";
        }

        try {
            return JsonFormat.printer().print(configuration);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to serialize task configuration", e);
        }
    }

    private com.example.nimbus.v1.TaskLifecycle toProtoLifecycle(TaskLifecycle lifecycle) {
        return com.example.nimbus.v1.TaskLifecycle.valueOf(lifecycle.name());
    }
}
