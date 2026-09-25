package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.WorkerInfo;
import com.example.scheduler.registry.InMemoryWorkerRegistry;
import com.example.scheduler.registry.WorkerRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class WorkerFailureDetectorTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");

    @Test
    void shouldKeepHealthyWorkerBeforeTimeout() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-healthy",
                List.of("ocr"),
                2,
                "host-healthy",
                WorkerState.AVAILABLE,
                NOW.minusSeconds(60),
                NOW.minusSeconds(5)
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        detector.detectFailures();

        assertEquals(WorkerState.AVAILABLE, registry.findById("worker-healthy").orElseThrow().state());
    }

    @Test
    void shouldMarkAvailableWorkerUnhealthyWhenHeartbeatExpired() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-available",
                List.of("ocr"),
                2,
                "host-available",
                WorkerState.AVAILABLE,
                NOW.minusSeconds(60),
                NOW.minusSeconds(31)
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        detector.detectFailures();

        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-available").orElseThrow().state());
    }

    @Test
    void shouldMarkBusyWorkerUnhealthyWhenHeartbeatExpired() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-busy",
                List.of("ocr"),
                2,
                "host-busy",
                WorkerState.BUSY,
                NOW.minusSeconds(60),
                NOW.minusSeconds(31),
                "task-1"
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        detector.detectFailures();

        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-busy").orElseThrow().state());
    }

    @Test
    void shouldNotMarkWorkerUnhealthyAtExactTimeoutBoundary() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-boundary",
                List.of("ocr"),
                2,
                "host-boundary",
                WorkerState.AVAILABLE,
                NOW.minusSeconds(60),
                NOW.minusSeconds(15)
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        detector.detectFailures();

        assertEquals(WorkerState.AVAILABLE, registry.findById("worker-boundary").orElseThrow().state());
    }

    @Test
    void shouldUseConfiguredTimeout() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-configured",
                List.of("ocr"),
                2,
                "host-configured",
                WorkerState.AVAILABLE,
                NOW.minusSeconds(60),
                NOW.minusSeconds(6)
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(5));

        detector.detectFailures();

        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-configured").orElseThrow().state());
    }

    @Test
    void shouldOnlyInspectRegisteredWorkers() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-known",
                List.of("ocr"),
                2,
                "host-known",
                WorkerState.AVAILABLE,
                NOW.minusSeconds(60),
                NOW.minusSeconds(31)
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        assertDoesNotThrow(detector::detectFailures);
        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-known").orElseThrow().state());
        assertFalse(registry.findById("unknown-worker").isPresent());
        assertEquals(1, registry.listAll().size());
        assertTrue(registry.listAll().stream().map(WorkerInfo::workerId).toList().contains("worker-known"));
        assertFalse(registry.listAll().stream().anyMatch(worker -> "unknown-worker".equals(worker.workerId())));
    }

    @Test
    void shouldRecoverUnhealthyWorkerToAvailableThroughHeartbeat() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-recover-available",
                List.of("ocr"),
                2,
                "host-recover-available",
                WorkerState.AVAILABLE,
                NOW.minusSeconds(60),
                NOW.minusSeconds(31)
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));
        detector.detectFailures();
        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-recover-available").orElseThrow().state());

        registry.updateHeartbeat("worker-recover-available", WorkerState.AVAILABLE, null, NOW.minusSeconds(1));

        assertEquals(WorkerState.AVAILABLE, registry.findById("worker-recover-available").orElseThrow().state());
    }

    @Test
    void shouldRecoverUnhealthyWorkerToBusyThroughHeartbeat() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-recover-busy",
                List.of("ocr"),
                2,
                "host-recover-busy",
                WorkerState.BUSY,
                NOW.minusSeconds(60),
                NOW.minusSeconds(31),
                "task-42"
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));
        detector.detectFailures();
        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-recover-busy").orElseThrow().state());

        registry.updateHeartbeat("worker-recover-busy", WorkerState.BUSY, "task-42", NOW.minusSeconds(1));

        assertEquals(WorkerState.BUSY, registry.findById("worker-recover-busy").orElseThrow().state());
        assertEquals("task-42", registry.findById("worker-recover-busy").orElseThrow().currentTaskId());
    }

    @Test
    void shouldNotMutateTaskStateWhenWorkerBecomesUnhealthy() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-task",
                List.of("ocr"),
                2,
                "host-task",
                WorkerState.BUSY,
                NOW.minusSeconds(60),
                NOW.minusSeconds(31),
                "task-running"
        ));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        detector.detectFailures();

        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-task").orElseThrow().state());
        assertEquals("task-running", registry.findById("worker-task").orElseThrow().currentTaskId());
    }

    @Test
    void shouldOnlyMarkTimedOutWorkers() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo("worker-a", List.of("ocr"), 2, "host-a", WorkerState.AVAILABLE, NOW.minusSeconds(60), NOW.minusSeconds(5)));
        registry.register(new WorkerInfo("worker-b", List.of("ocr"), 2, "host-b", WorkerState.AVAILABLE, NOW.minusSeconds(60), NOW.minusSeconds(31)));
        registry.register(new WorkerInfo("worker-c", List.of("ocr"), 2, "host-c", WorkerState.BUSY, NOW.minusSeconds(60), NOW.minusSeconds(2), "task-7"));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        detector.detectFailures();

        assertEquals(WorkerState.AVAILABLE, registry.findById("worker-a").orElseThrow().state());
        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-b").orElseThrow().state());
        assertEquals(WorkerState.BUSY, registry.findById("worker-c").orElseThrow().state());
    }

    @Test
    void shouldBeSafeWhenRunningRepeatedDetectionOnUnhealthyWorker() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo("worker-repeat", List.of("ocr"), 2, "host-repeat", WorkerState.UNHEALTHY, NOW.minusSeconds(60), NOW.minusSeconds(31)));

        WorkerFailureDetector detector = new WorkerFailureDetector(registry, Clock.fixed(NOW, ZoneOffset.UTC), Duration.ofSeconds(15));

        assertDoesNotThrow(detector::detectFailures);
        assertDoesNotThrow(detector::detectFailures);
        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-repeat").orElseThrow().state());
    }

    @Test
    void shouldTriggerReassignmentForUnhealthyWorker() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-reassign",
                List.of("ocr"),
                2,
                "host-reassign",
                WorkerState.AVAILABLE,
                NOW.minusSeconds(60),
                NOW.minusSeconds(31)
        ));

        WorkerTaskReassignmentService reassignmentService = mock(WorkerTaskReassignmentService.class);
        WorkerFailureDetector detector = new WorkerFailureDetector(
                registry,
                Clock.fixed(NOW, ZoneOffset.UTC),
                Duration.ofSeconds(15),
                reassignmentService
        );

        detector.detectFailures();

        verify(reassignmentService).reassignTasksForUnhealthyWorker("worker-reassign");
        assertEquals(WorkerState.UNHEALTHY, registry.findById("worker-reassign").orElseThrow().state());
    }
}
