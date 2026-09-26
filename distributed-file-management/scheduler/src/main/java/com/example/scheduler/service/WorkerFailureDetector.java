package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.WorkerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class WorkerFailureDetector {

    private static final Logger log = LoggerFactory.getLogger(WorkerFailureDetector.class);
    private static final Set<WorkerState> DETECTABLE_STATES = Set.of(WorkerState.AVAILABLE, WorkerState.BUSY);

    private final WorkerRegistry workerRegistry;
    private final Clock clock;
    private final Duration heartbeatTimeout;
    private final WorkerTaskReassignmentService reassignmentService;
    private final Set<String> warnedWorkers = ConcurrentHashMap.newKeySet();

    @Autowired
    public WorkerFailureDetector(WorkerRegistry workerRegistry,
            @Value("${worker.heartbeat.timeout-ms:15000}") long heartbeatTimeoutMs) {
        this(workerRegistry, Clock.systemUTC(), Duration.ofMillis(heartbeatTimeoutMs), null);
    }

    public WorkerFailureDetector(WorkerRegistry workerRegistry, Clock clock, Duration heartbeatTimeout) {
        this(workerRegistry, clock, heartbeatTimeout, null);
    }

    public WorkerFailureDetector(WorkerRegistry workerRegistry, Clock clock, Duration heartbeatTimeout,
            WorkerTaskReassignmentService reassignmentService) {
        this.workerRegistry = workerRegistry;
        this.clock = clock;
        this.heartbeatTimeout = heartbeatTimeout == null ? Duration.ofMillis(15000) : heartbeatTimeout;
        this.reassignmentService = reassignmentService;
    }

    @Scheduled(fixedDelayString = "${worker.heartbeat.failure-check-ms:5000}")
    public void detectFailures() {
        if (workerRegistry == null) {
            return;
        }

        Instant now = Instant.now(clock);
        List<WorkerInfo> before = workerRegistry.listAll();
        workerRegistry.markUnhealthyIfExpired(now, heartbeatTimeout);

        for (WorkerInfo worker : before) {
            if (worker == null || worker.lastHeartbeatAt() == null || !DETECTABLE_STATES.contains(worker.state())) {
                continue;
            }

            Duration elapsed = Duration.between(worker.lastHeartbeatAt(), now);
            if (elapsed.compareTo(heartbeatTimeout) > 0) {
                WorkerInfo updated = workerRegistry.findById(worker.workerId()).orElse(null);
                if (updated != null && updated.state() == WorkerState.UNHEALTHY) {
                    if (warnedWorkers.add(worker.workerId())) {
                        log.info("Worker marked unhealthy: workerId={}, lastHeartbeatAt={}, timeoutMs={}",
                                worker.workerId(), worker.lastHeartbeatAt(), heartbeatTimeout.toMillis());
                    }
                    if (reassignmentService != null) {
                        reassignmentService.reassignTasksForUnhealthyWorker(worker.workerId());
                    }
                }
            }
        }
    }

    public void markUnhealthyIfExpired() {
        detectFailures();
    }
}
