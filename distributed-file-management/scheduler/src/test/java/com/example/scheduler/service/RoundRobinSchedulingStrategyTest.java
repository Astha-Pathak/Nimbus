package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerState;
import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import com.example.scheduler.domain.WorkerInfo;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RoundRobinSchedulingStrategyTest {

    private final RoundRobinSchedulingStrategy strategy = new RoundRobinSchedulingStrategy();

    @Test
    void shouldRoundRobinAcrossAvailableEligibleWorkers() {
        List<WorkerInfo> workers = List.of(
                worker("w1", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w2", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w3", List.of("checksum"), WorkerState.AVAILABLE));

        Task task1 = task("checksum");
        Task task2 = task("checksum");
        Task task3 = task("checksum");
        Task task4 = task("checksum");

        assertEquals("w1", strategy.selectWorker(task1, workers).orElseThrow().workerId());
        assertEquals("w2", strategy.selectWorker(task2, workers).orElseThrow().workerId());
        assertEquals("w3", strategy.selectWorker(task3, workers).orElseThrow().workerId());
        assertEquals("w1", strategy.selectWorker(task4, workers).orElseThrow().workerId());
    }

    @Test
    void shouldReturnEmptyWhenNoWorkersAvailable() {
        assertTrue(strategy.selectWorker(task("checksum"), List.of()).isEmpty());
    }

    @Test
    void shouldIgnoreUnavailableWorkers() {
        List<WorkerInfo> workers = List.of(
                worker("w1", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w2", List.of("checksum"), WorkerState.BUSY),
                worker("w3", List.of("checksum"), WorkerState.AVAILABLE));

        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w3", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
    }

    @Test
    void shouldRespectProcessorCompatibility() {
        List<WorkerInfo> workers = List.of(
                worker("w1", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w2", List.of("compression"), WorkerState.AVAILABLE),
                worker("w3", List.of("checksum"), WorkerState.AVAILABLE));

        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w3", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
    }

    @Test
    void shouldReturnEmptyWhenNoWorkerSupportsProcessor() {
        List<WorkerInfo> workers = List.of(
                worker("w1", List.of("compression"), WorkerState.AVAILABLE),
                worker("w2", List.of("compression"), WorkerState.AVAILABLE));

        assertTrue(strategy.selectWorker(task("checksum"), workers).isEmpty());
    }

    @Test
    void shouldSkipUnavailableWorkersWithoutBreakingSequence() {
        List<WorkerInfo> workers = List.of(
                worker("w1", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w2", List.of("checksum"), WorkerState.BUSY),
                worker("w3", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w4", List.of("checksum"), WorkerState.AVAILABLE));

        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w3", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w4", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
    }

    @Test
    void shouldUseDeterministicWorkerOrdering() {
        List<WorkerInfo> workers = List.of(
                worker("w3", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w1", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w2", List.of("checksum"), WorkerState.AVAILABLE));

        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w2", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w3", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
    }

    @Test
    void shouldCycleCorrectlyAcrossRepeatedCalls() {
        List<WorkerInfo> workers = List.of(
                worker("w1", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w2", List.of("checksum"), WorkerState.AVAILABLE),
                worker("w3", List.of("checksum"), WorkerState.AVAILABLE));

        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w2", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w3", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w1", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
        assertEquals("w2", strategy.selectWorker(task("checksum"), workers).orElseThrow().workerId());
    }

    private static WorkerInfo worker(String workerId, List<String> processorTypes, WorkerState state) {
        Instant now = Instant.now();
        return new WorkerInfo(workerId, processorTypes, 2, "host-" + workerId, state, now, now);
    }

    private static Task task(String processorType) {
        Task task = new Task("req-" + processorType + "-" + System.nanoTime(), "file-1", processorType,
                "{}", TaskLifecycle.QUEUED, null, 0, 3, null, null);
        return task;
    }
}
