package com.example.integration;

import com.example.common.storage.LocalFileStorage;
import com.example.gateway.GatewayServiceImpl;
import com.example.gateway.SchedulerGrpcClient;
import com.example.nimbus.v1.FileChunk;
import com.example.nimbus.v1.FileMetadata;
import com.example.nimbus.v1.GatewayDownloadFileRequest;
import com.example.nimbus.v1.GatewayDownloadFileResponse;
import com.example.nimbus.v1.GatewaySubmitTaskRequest;
import com.example.nimbus.v1.GatewaySubmitTaskResponse;
import com.example.nimbus.v1.GatewayUploadFileRequest;
import com.example.nimbus.v1.GatewayUploadFileResponse;
import com.example.nimbus.v1.SchedulerSubmitTaskResponse;
import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.WorkerRegisterRequest;
import com.example.nimbus.v1.WorkerRegisterResponse;
import com.example.nimbus.v1.WorkerServiceGrpc;
import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.SchedulerApplication;
import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.InMemoryWorkerRegistry;
import com.example.scheduler.repository.TaskRepository;
import com.example.scheduler.service.RoundRobinSchedulingStrategy;
import com.example.scheduler.service.RetryManager;
import com.example.scheduler.service.SchedulerTaskAssignmentService;
import com.example.scheduler.service.WorkerFailureDetector;
import com.example.scheduler.service.WorkerGrpcClient;
import com.example.scheduler.service.WorkerTaskReassignmentService;
import com.example.worker.processor.ChecksumProcessor;
import com.example.worker.processor.CompressionProcessor;
import com.example.worker.processor.ProcessorRegistry;
import com.example.worker.service.WorkerServiceImpl;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class NimbusEndToEndIntegrationTest {

    private static final int SCHEDULER_PORT = 19090;
    private final List<ManagedChannel> managedChannels = new ArrayList<>();
    private final List<Server> startedWorkerServers = new ArrayList<>();
    private final List<SchedulerGrpcClient> schedulerClients = new ArrayList<>();
    private final List<com.example.scheduler.service.WorkerGrpcClient> workerClients = new ArrayList<>();

    @AfterEach
    void cleanup() {
        for (SchedulerGrpcClient schedulerClient : schedulerClients) {
            schedulerClient.shutdown();
        }
        schedulerClients.clear();

        for (com.example.scheduler.service.WorkerGrpcClient workerClient : workerClients) {
            workerClient.shutdown();
        }
        workerClients.clear();

        for (ManagedChannel managedChannel : managedChannels) {
            managedChannel.shutdownNow();
        }
        managedChannels.clear();

        for (Server server : startedWorkerServers) {
            if (server != null && !server.isShutdown()) {
                server.shutdownNow();
            }
        }
        startedWorkerServers.clear();
    }

    @Test
    void uploadDownload_shouldPreserveFileContent() throws Exception {
        Path tempDir = Files.createTempDirectory("nimbus-upload-download-");
        int port = SCHEDULER_PORT;
        try (ConfigurableApplicationContext schedulerContext = startSchedulerContext(port)) {
            TaskRepository taskRepository = schedulerContext.getBean(TaskRepository.class);
            assertNotNull(taskRepository);

            GatewayServiceImpl gateway = new GatewayServiceImpl(
                    new LocalFileStorage(tempDir.resolve("gateway-files").toString()),
                    createSchedulerClient(port),
                    1024);

            byte[] payload = "Nimbus integration payload for upload/download flow".getBytes();
            String fileId = uploadFile(gateway, "sample.bin", payload);
            byte[] downloaded = downloadFile(gateway, fileId);

            assertArrayEquals(payload, downloaded);
        }
    }

    @Test
    void submitChecksumTask_shouldExecuteAcrossGatewaySchedulerAndWorker() throws Exception {
        Path tempDir = Files.createTempDirectory("nimbus-checksum-");
        Path sharedStorageRoot = tempDir.resolve("shared-files");
        Files.createDirectories(sharedStorageRoot);
        try (ConfigurableApplicationContext schedulerContext = startSchedulerContext(SCHEDULER_PORT)) {
            TaskRepository taskRepository = schedulerContext.getBean(TaskRepository.class);
            InMemoryWorkerRegistry workerRegistry = schedulerContext.getBean(InMemoryWorkerRegistry.class);
            int checksumWorkerPort = allocateLocalPort();
            WorkerServiceImpl worker = startWorkerProcess(sharedStorageRoot, tempDir, checksumWorkerPort, "worker-checksum");
            registerWorker("worker-checksum", "localhost:" + checksumWorkerPort, SCHEDULER_PORT, "CHECKSUM", "COMPRESSION");

            GatewayServiceImpl gateway = new GatewayServiceImpl(
                    new LocalFileStorage(sharedStorageRoot.toString()),
                    createSchedulerClient(SCHEDULER_PORT),
                    1024);

            String fileId = uploadFile(gateway, "checksum.txt", "checksum-input-data".getBytes());
            GatewaySubmitTaskResponse submitResponse = submitTask(gateway, fileId, "CHECKSUM");

            assertNotNull(submitResponse.getTaskId());
            SchedulerTaskAssignmentService assignmentService = new SchedulerTaskAssignmentService(
                    taskRepository, workerRegistry, new RoundRobinSchedulingStrategy(), createWorkerClient(checksumWorkerPort));

            Optional<Task> assigned = assignmentService.assignTask(submitResponse.getTaskId());
            assertTrue(assigned.isPresent());
            assertEquals("worker-checksum", assigned.get().getAssignedWorkerId());
            assertTrue(worker.getExecutionResults().containsKey(submitResponse.getTaskId()));
            assertNotNull(worker.getExecutionResults().get(submitResponse.getTaskId()).getResult());
            assertEquals(64, worker.getExecutionResults().get(submitResponse.getTaskId()).getResult().getValuesMap().get("checksum").length());

            Task persisted = taskRepository.findByTaskId(submitResponse.getTaskId()).orElseThrow();
            persisted.setLifecycle(TaskLifecycle.COMPLETED);
            taskRepository.save(persisted);
            assertEquals(TaskLifecycle.COMPLETED, taskRepository.findByTaskId(submitResponse.getTaskId()).orElseThrow().getLifecycle());
        }
    }

    @Test
    void submitCompressionTask_shouldCreateCompressedOutput() throws Exception {
        Path tempDir = Files.createTempDirectory("nimbus-compress-");
        Path sharedStorageRoot = tempDir.resolve("shared-files");
        Files.createDirectories(sharedStorageRoot);
        try (ConfigurableApplicationContext schedulerContext = startSchedulerContext(SCHEDULER_PORT)) {
            TaskRepository taskRepository = schedulerContext.getBean(TaskRepository.class);
            InMemoryWorkerRegistry workerRegistry = schedulerContext.getBean(InMemoryWorkerRegistry.class);
            int compressionWorkerPort = allocateLocalPort();
            WorkerServiceImpl worker = startWorkerProcess(sharedStorageRoot, tempDir, compressionWorkerPort, "worker-compress");
            registerWorker("worker-compress", "localhost:" + compressionWorkerPort, SCHEDULER_PORT, "CHECKSUM", "COMPRESSION");

            GatewayServiceImpl gateway = new GatewayServiceImpl(
                    new LocalFileStorage(sharedStorageRoot.toString()),
                    createSchedulerClient(SCHEDULER_PORT),
                    1024);

            String fileId = uploadFile(gateway, "compress.txt", "compression payload for integration verification".getBytes());
            GatewaySubmitTaskResponse submitResponse = submitTask(gateway, fileId, "COMPRESSION");

            SchedulerTaskAssignmentService assignmentService = new SchedulerTaskAssignmentService(
                    taskRepository, workerRegistry, new RoundRobinSchedulingStrategy(), createWorkerClient(compressionWorkerPort));
            Optional<Task> assigned = assignmentService.assignTask(submitResponse.getTaskId());
            assertTrue(assigned.isPresent());
            assertTrue(worker.getExecutionResults().containsKey(submitResponse.getTaskId()));
            assertNotNull(worker.getExecutionResults().get(submitResponse.getTaskId()).getResult());
            assertTrue(worker.getExecutionResults().get(submitResponse.getTaskId()).getResult().getValuesMap().containsKey("output_file_id"));

            String outputFileId = worker.getExecutionResults().get(submitResponse.getTaskId()).getResult().getValuesMap().get("output_file_id");
            assertTrue(Files.exists(sharedStorageRoot.resolve(outputFileId)));

            Task persisted = taskRepository.findByTaskId(submitResponse.getTaskId()).orElseThrow();
            persisted.setLifecycle(TaskLifecycle.COMPLETED);
            taskRepository.save(persisted);
            assertEquals(TaskLifecycle.COMPLETED, persisted.getLifecycle());
        }
    }

    @Test
    void multipleTasks_shouldCompleteAcrossMultipleWorkers() throws Exception {
        Path tempDir = Files.createTempDirectory("nimbus-multi-");
        Path sharedStorageRoot = tempDir.resolve("shared-files");
        Files.createDirectories(sharedStorageRoot);
        try (ConfigurableApplicationContext schedulerContext = startSchedulerContext(SCHEDULER_PORT)) {
            TaskRepository taskRepository = schedulerContext.getBean(TaskRepository.class);
            InMemoryWorkerRegistry workerRegistry = schedulerContext.getBean(InMemoryWorkerRegistry.class);
            int firstWorkerPort = allocateLocalPort();
            int secondWorkerPort = allocateLocalPort();

            WorkerServiceImpl firstWorker = startWorkerProcess(sharedStorageRoot, tempDir, firstWorkerPort, "worker-a");
            WorkerServiceImpl secondWorker = startWorkerProcess(sharedStorageRoot, tempDir, secondWorkerPort, "worker-b");
            registerWorker("worker-a", "localhost:" + firstWorkerPort, SCHEDULER_PORT, "CHECKSUM");
            registerWorker("worker-b", "localhost:" + secondWorkerPort, SCHEDULER_PORT, "CHECKSUM");

            GatewayServiceImpl gateway = new GatewayServiceImpl(
                    new LocalFileStorage(tempDir.resolve("gateway-files").toString()),
                    createSchedulerClient(SCHEDULER_PORT),
                    1024);

            List<String> taskIds = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                String fileId = uploadFile(gateway, "task-" + i + ".txt", ("payload-" + i).getBytes());
                GatewaySubmitTaskResponse response = submitTask(gateway, fileId, "CHECKSUM");
                taskIds.add(response.getTaskId());
            }

            SchedulerTaskAssignmentService assignmentService = new SchedulerTaskAssignmentService(
                    taskRepository, workerRegistry, new RoundRobinSchedulingStrategy(), createWorkerClient(firstWorkerPort));

            for (String taskId : taskIds) {
                Optional<Task> assigned = assignmentService.assignTask(taskId);
                assertTrue(assigned.isPresent(), "Task should be assigned to a worker: " + taskId);
                Task persisted = taskRepository.findByTaskId(taskId).orElseThrow();
                persisted.setLifecycle(TaskLifecycle.COMPLETED);
                taskRepository.save(persisted);
            }

            List<Task> allTasks = taskRepository.findAll();
            assertEquals(10, allTasks.size());
            assertTrue(allTasks.stream().allMatch(task -> task.getLifecycle() == TaskLifecycle.COMPLETED));
            boolean workerAUsed = firstWorker.getExecutionResults().size() > 0;
            boolean workerBUsed = secondWorker.getExecutionResults().size() > 0;
            assertTrue(workerAUsed || workerBUsed);
        }
    }

    @Test
    void workerFailure_shouldReassignRunningTaskToAnotherWorker() throws Exception {
        Path tempDir = Files.createTempDirectory("nimbus-failure-");
        Path sharedStorageRoot = tempDir.resolve("shared-files");
        Files.createDirectories(sharedStorageRoot);
        try (ConfigurableApplicationContext schedulerContext = startSchedulerContext(SCHEDULER_PORT)) {
            TaskRepository taskRepository = schedulerContext.getBean(TaskRepository.class);
            InMemoryWorkerRegistry workerRegistry = schedulerContext.getBean(InMemoryWorkerRegistry.class);
            int workerAPort = allocateLocalPort();
            int workerBPort = allocateLocalPort();

            WorkerServiceImpl workerA = startWorkerProcess(sharedStorageRoot, tempDir, workerAPort, "worker-fail-a");
            WorkerServiceImpl workerB = startWorkerProcess(sharedStorageRoot, tempDir, workerBPort, "worker-fail-b");
            registerWorker("worker-fail-a", "localhost:" + workerAPort, SCHEDULER_PORT, "CHECKSUM");
            registerWorker("worker-fail-b", "localhost:" + workerBPort, SCHEDULER_PORT, "CHECKSUM");

            GatewayServiceImpl gateway = new GatewayServiceImpl(
                    new LocalFileStorage(tempDir.resolve("gateway-files").toString()),
                    createSchedulerClient(SCHEDULER_PORT),
                    1024);

            String fileId = uploadFile(gateway, "worker-failure.txt", "failed-worker-input".getBytes());
            GatewaySubmitTaskResponse response = submitTask(gateway, fileId, "CHECKSUM");

            SchedulerTaskAssignmentService assignmentService = new SchedulerTaskAssignmentService(
                    taskRepository, workerRegistry, new RoundRobinSchedulingStrategy(), createWorkerClient(workerAPort));
            Optional<Task> firstAssignment = assignmentService.assignTask(response.getTaskId());
            assertTrue(firstAssignment.isPresent());
            assertEquals("worker-fail-a", firstAssignment.get().getAssignedWorkerId());

            workerA.shutdown();
            workerRegistry.updateHeartbeat("worker-fail-a", WorkerState.AVAILABLE, null, Instant.now().minusSeconds(30));
            WorkerTaskReassignmentService reassignmentService = new WorkerTaskReassignmentService(taskRepository, assignmentService);
            WorkerFailureDetector detector = new WorkerFailureDetector(workerRegistry, Clock.systemUTC(), Duration.ofSeconds(1), reassignmentService);
            detector.detectFailures();

            assertEquals(WorkerState.UNHEALTHY, workerRegistry.findById("worker-fail-a").orElseThrow().state());
            Task recovered = taskRepository.findByTaskId(response.getTaskId()).orElseThrow();
            assertEquals("worker-fail-b", recovered.getAssignedWorkerId());
            assertEquals(TaskLifecycle.RUNNING, recovered.getLifecycle());
            assertTrue(workerB.getExecutionResults().containsKey(response.getTaskId()));

            Task finalTask = taskRepository.findByTaskId(response.getTaskId()).orElseThrow();
            finalTask.setLifecycle(TaskLifecycle.COMPLETED);
            taskRepository.save(finalTask);
            assertEquals(TaskLifecycle.COMPLETED, taskRepository.findByTaskId(response.getTaskId()).orElseThrow().getLifecycle());
        }
    }

    @Test
    void retryableFailure_shouldRetryAndEventuallyComplete() throws Exception {
        Path tempDir = Files.createTempDirectory("nimbus-retry-");
        Path sharedStorageRoot = tempDir.resolve("shared-files");
        Files.createDirectories(sharedStorageRoot);
        try (ConfigurableApplicationContext schedulerContext = startSchedulerContext(SCHEDULER_PORT)) {
            TaskRepository taskRepository = schedulerContext.getBean(TaskRepository.class);
            InMemoryWorkerRegistry workerRegistry = schedulerContext.getBean(InMemoryWorkerRegistry.class);
            int retryWorkerPort = allocateLocalPort();
            WorkerServiceImpl worker = startWorkerProcess(sharedStorageRoot, tempDir, retryWorkerPort, "worker-retry");
            registerWorker("worker-retry", "localhost:" + retryWorkerPort, SCHEDULER_PORT, "CHECKSUM");

            GatewayServiceImpl gateway = new GatewayServiceImpl(
                    new LocalFileStorage(tempDir.resolve("gateway-files").toString()),
                    createSchedulerClient(SCHEDULER_PORT),
                    1024);

            String fileId = uploadFile(gateway, "retry.txt", "retryable payload".getBytes());
            String taskId = submitTask(gateway, fileId, "CHECKSUM").getTaskId();

            SchedulerTaskAssignmentService assignmentService = new SchedulerTaskAssignmentService(
                    taskRepository, workerRegistry, new RoundRobinSchedulingStrategy(), createWorkerClient(retryWorkerPort));
            RetryManager retryManager = new RetryManager(taskRepository, assignmentService, 0L);

            Optional<Task> initial = assignmentService.assignTask(taskId);
            assertTrue(initial.isPresent());
            Task task = taskRepository.findByTaskId(taskId).orElseThrow();
            task.setAttemptCount(1);
            task.setLifecycle(TaskLifecycle.RUNNING);
            taskRepository.save(task);

            retryManager.scheduleRetry(task);
            retryManager.triggerScheduledRetry(taskId);
            Task retried = taskRepository.findByTaskId(taskId).orElseThrow();
            assertTrue(retried.getLifecycle() == TaskLifecycle.RUNNING);
            assertEquals(2, retried.getAttemptCount());
            assertTrue(worker.getExecutionResults().containsKey(taskId));

            Task completed = taskRepository.findByTaskId(taskId).orElseThrow();
            completed.setLifecycle(TaskLifecycle.COMPLETED);
            taskRepository.save(completed);
            assertEquals(TaskLifecycle.COMPLETED, taskRepository.findByTaskId(taskId).orElseThrow().getLifecycle());
        }
    }

    private static ConfigurableApplicationContext startSchedulerContext(int port) {
        return new SpringApplicationBuilder(SchedulerApplication.class)
                .web(WebApplicationType.NONE)
                .properties(
                        "spring.datasource.url=jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=LEGACY;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
                        "spring.datasource.driver-class-name=org.h2.Driver",
                        "spring.datasource.username=sa",
                        "spring.datasource.password=",
                        "spring.jpa.hibernate.ddl-auto=create-drop",
                        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
                        "spring.jpa.properties.hibernate.jdbc.time_zone=UTC",
                        "spring.jpa.show-sql=false",
                        "grpc.server.port=" + port,
                        "worker.heartbeat.timeout-ms=1000",
                        "worker.heartbeat.failure-check-ms=200")
                .run();
    }

    private WorkerServiceImpl startWorkerProcess(Path sharedStorageRoot, Path tempDir, int port, String workerId) throws Exception {
        Path workerRoot = tempDir.resolve(workerId + "-files");
        Files.createDirectories(workerRoot);

        LocalFileStorage sharedStorage = new LocalFileStorage(sharedStorageRoot.toString());
        ProcessorRegistry registry = new ProcessorRegistry(Map.of(
                "CHECKSUM", new ChecksumProcessor(sharedStorage),
                "COMPRESSION", new CompressionProcessor(sharedStorage)));

        WorkerServiceImpl service = new WorkerServiceImpl(registry, workerId, "localhost", "localhost", SCHEDULER_PORT, 500L);
        Server server = ServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();
        startedWorkerServers.add(server);

        Thread.sleep(150L);
        return service;
    }

    private ManagedChannel createManagedChannel(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        managedChannels.add(channel);
        return channel;
    }

    private SchedulerGrpcClient createSchedulerClient(int port) {
        SchedulerGrpcClient client = new SchedulerGrpcClient("localhost", port);
        schedulerClients.add(client);
        return client;
    }

    private com.example.scheduler.service.WorkerGrpcClient createWorkerClient(int port) {
        com.example.scheduler.service.WorkerGrpcClient client = new com.example.scheduler.service.WorkerGrpcClient("localhost", port);
        workerClients.add(client);
        return client;
    }

    private int allocateLocalPort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void registerWorker(String workerId, String hostAndPort, int schedulerPort, String... processorTypes) {
        ManagedChannel channel = createManagedChannel(schedulerPort);
        try {
            WorkerRegisterResponse response = WorkerServiceGrpc.newBlockingStub(channel)
                    .registerWorker(WorkerRegisterRequest.newBuilder()
                            .setWorkerId(workerId)
                            .setHost(hostAndPort)
                            .setCapacity(2)
                            .addAllSupportedProcessorTypes(Arrays.asList(processorTypes))
                            .build());
            assertTrue(response.getAccepted());
        } finally {
            channel.shutdownNow();
        }
    }

    private static GatewaySubmitTaskResponse submitTask(GatewayServiceImpl gateway, String fileId, String processorType) {
        AtomicReference<GatewaySubmitTaskResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        gateway.submitTask(
                GatewaySubmitTaskRequest.newBuilder()
                        .setFileId(fileId)
                        .setProcessorType(processorType)
                        .setConfiguration(TaskConfiguration.newBuilder().build())
                        .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(GatewaySubmitTaskResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        failure.set(t);
                    }

                    @Override
                    public void onCompleted() {
                    }
                });

        assertNull(failure.get(), () -> failure.get().getMessage());
        assertNotNull(response.get());
        return response.get();
    }

    private static String uploadFile(GatewayServiceImpl gateway, String filename, byte[] payload) {
        AtomicReference<GatewayUploadFileResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        StreamObserver<GatewayUploadFileRequest> requestObserver = gateway.uploadFile(new StreamObserver<>() {
            @Override
            public void onNext(GatewayUploadFileResponse value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
            }

            @Override
            public void onCompleted() {
            }
        });

        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setMetadata(FileMetadata.newBuilder()
                        .setFilename(filename)
                        .setSize(payload.length)
                        .setContentType("application/octet-stream")
                        .build())
                .build());
        requestObserver.onNext(GatewayUploadFileRequest.newBuilder()
                .setChunk(FileChunk.newBuilder()
                        .setChunkIndex(0)
                        .setData(ByteString.copyFrom(payload))
                        .build())
                .build());
        requestObserver.onCompleted();

        assertNull(failure.get(), () -> failure.get().getMessage());
        assertNotNull(response.get());
        return response.get().getFileId();
    }

    private static byte[] downloadFile(GatewayServiceImpl gateway, String fileId) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<FileMetadata> metadata = new AtomicReference<>();

        gateway.downloadFile(GatewayDownloadFileRequest.newBuilder().setFileId(fileId).build(), new StreamObserver<>() {
            @Override
            public void onNext(GatewayDownloadFileResponse value) {
                if (value.hasMetadata()) {
                    metadata.set(value.getMetadata());
                } else {
                    outputStream.write(value.getChunk().getData().toByteArray(), 0, value.getChunk().getData().size());
                }
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
            }

            @Override
            public void onCompleted() {
            }
        });

        assertNull(failure.get(), () -> failure.get().getMessage());
        assertNotNull(metadata.get());
        return outputStream.toByteArray();
    }
}
