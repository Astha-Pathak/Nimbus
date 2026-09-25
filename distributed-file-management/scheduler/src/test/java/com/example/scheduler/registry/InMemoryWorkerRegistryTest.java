package com.example.scheduler.registry;

import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.WorkerInfo;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryWorkerRegistryTest {

    @Test
    void shouldRegisterAndFindAvailableWorkers() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        WorkerInfo worker = new WorkerInfo(
                "worker-1",
                List.of("ocr", "resize"),
                3,
                "worker-1.internal",
                WorkerState.AVAILABLE,
                Instant.now(),
                Instant.now());

        registry.register(worker);

        assertTrue(registry.findById("worker-1").isPresent());
        assertEquals(1, registry.listAvailable().size());
        assertEquals("worker-1", registry.listAvailable().get(0).workerId());
    }

    @Test
    void shouldOnlyListAvailableWorkers() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        registry.register(new WorkerInfo(
                "worker-1",
                List.of("ocr"),
                2,
                "host-a",
                WorkerState.AVAILABLE,
                Instant.now(),
                Instant.now()));
        registry.register(new WorkerInfo(
                "worker-2",
                List.of("ocr"),
                2,
                "host-b",
                WorkerState.BUSY,
                Instant.now(),
                Instant.now()));
        registry.register(new WorkerInfo(
                "worker-3",
                List.of("ocr"),
                2,
                "host-c",
                WorkerState.UNHEALTHY,
                Instant.now(),
                Instant.now()));

        List<WorkerInfo> available = registry.listAvailable();
        assertEquals(1, available.size());
        assertEquals("worker-1", available.get(0).workerId());
    }

    @Test
    void shouldUpsertSameWorkerIdWithoutDuplicateEntries() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        Instant firstRegisteredAt = Instant.parse("2024-01-01T00:00:00Z");
        Instant firstHeartbeat = Instant.parse("2024-01-01T00:00:01Z");

        registry.register(new WorkerInfo(
                "worker-9",
                List.of("ocr"),
                2,
                "host-a",
                WorkerState.AVAILABLE,
                firstRegisteredAt,
                firstHeartbeat));

        Instant secondRegisteredAt = Instant.parse("2024-01-01T00:05:00Z");
        Instant secondHeartbeat = Instant.parse("2024-01-01T00:05:05Z");
        registry.register(new WorkerInfo(
                "worker-9",
                List.of("ocr", "resize"),
                5,
                "host-b",
                WorkerState.AVAILABLE,
                secondRegisteredAt,
                secondHeartbeat));

        assertEquals(1, registry.listAvailable().size());
        WorkerInfo updated = registry.findById("worker-9").orElseThrow();
        assertEquals("worker-9", updated.workerId());
        assertEquals(List.of("ocr", "resize"), updated.supportedProcessorTypes());
        assertEquals(5, updated.capacity());
        assertEquals("host-b", updated.host());
        assertEquals(WorkerState.AVAILABLE, updated.state());
        assertEquals(secondRegisteredAt, updated.registeredAt());
        assertEquals(secondHeartbeat, updated.lastHeartbeatAt());
    }

    @Test
    void shouldUpdateLastHeartbeatAndCurrentTaskIdOnHeartbeat() {
        WorkerRegistry registry = new InMemoryWorkerRegistry();
        Instant registeredAt = Instant.parse("2024-01-01T00:00:00Z");
        registry.register(new WorkerInfo(
                "worker-heartbeat",
                List.of("ocr"),
                2,
                "host-heartbeat",
                WorkerState.AVAILABLE,
                registeredAt,
                registeredAt,
                "task-old"));

        Instant heartbeatAt = Instant.parse("2024-01-01T00:01:00Z");
        registry.updateHeartbeat("worker-heartbeat", WorkerState.BUSY, "task-new", heartbeatAt);

        WorkerInfo updated = registry.findById("worker-heartbeat").orElseThrow();
        assertEquals(WorkerState.BUSY, updated.state());
        assertEquals(heartbeatAt, updated.lastHeartbeatAt());
        assertEquals("task-new", updated.currentTaskId());
    }
}
