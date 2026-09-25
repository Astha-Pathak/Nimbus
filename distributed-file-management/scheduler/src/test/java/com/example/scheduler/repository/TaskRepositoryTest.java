
package com.example.scheduler.repository;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
class TaskRepositoryTest {

    @Autowired
    private TaskRepository taskRepository;

    @Test
    void shouldSaveAndRetrieveTask() {
        Task task = new Task(
                "req-1",
                "file-1",
                "ocr",
                "{\"mode\":\"fast\"}",
                TaskLifecycle.QUEUED,
                null,
                0,
                3,
                "INVALID_FILE",
                "File missing");

        Task saved = taskRepository.save(task);
        Task found = taskRepository.findByTaskId(saved.getTaskId()).orElseThrow();

        assertEquals("req-1", found.getRequestId());
        assertEquals("file-1", found.getFileId());
        assertEquals("ocr", found.getProcessorType());
        assertEquals(TaskLifecycle.QUEUED, found.getLifecycle());
    }

    @Test
    void shouldPersistTaskStatus() {
        Task task = new Task("req-2", "file-2", "resize", "{}", TaskLifecycle.RUNNING, "worker-1", 1, 5, null, null);

        Task saved = taskRepository.save(task);
        assertEquals(TaskLifecycle.RUNNING,
                taskRepository.findByTaskId(saved.getTaskId()).orElseThrow().getLifecycle());
    }

    @Test
    void shouldEnforceRequestIdUniqueness() {
        Task taskOne = new Task("req-3", "file-3", "ocr", "{}", TaskLifecycle.QUEUED, null, 0, 3, null, null);
        taskRepository.saveAndFlush(taskOne);

        Task taskTwo = new Task("req-3", "file-4", "ocr", "{}", TaskLifecycle.QUEUED, null, 0, 3, null, null);

        assertThrows(DataIntegrityViolationException.class, () -> taskRepository.saveAndFlush(taskTwo));
    }

    @Test
    void shouldFindTaskByRequestId() {
        Task task = new Task("req-4", "file-4", "ocr", "{}", TaskLifecycle.QUEUED, null, 0, 3, null, null);
        taskRepository.save(task);

        Task found = taskRepository.findByRequestId("req-4").orElseThrow();
        assertEquals("file-4", found.getFileId());
    }

    @Test
    void shouldFindQueuedTasks() {
        taskRepository.save(new Task("req-5", "file-5", "ocr", "{}", TaskLifecycle.QUEUED, null, 0, 3, null, null));
        taskRepository
                .save(new Task("req-6", "file-6", "ocr", "{}", TaskLifecycle.RUNNING, "worker-2", 1, 3, null, null));
        taskRepository
                .save(new Task("req-7", "file-7", "ocr", "{}", TaskLifecycle.COMPLETED, "worker-3", 1, 3, null, null));

        List<Task> queued = taskRepository.findByLifecycleOrderByCreatedAtAsc(TaskLifecycle.QUEUED);
        assertEquals(1, queued.size());
        assertEquals("req-5", queued.get(0).getRequestId());
    }

    @Test
    void shouldFindRunningTasks() {
        taskRepository.save(new Task("req-8", "file-8", "ocr", "{}", TaskLifecycle.QUEUED, null, 0, 3, null, null));
        taskRepository
                .save(new Task("req-9", "file-9", "ocr", "{}", TaskLifecycle.RUNNING, "worker-4", 1, 3, null, null));
        taskRepository
                .save(new Task("req-10", "file-10", "ocr", "{}", TaskLifecycle.RUNNING, "worker-5", 2, 3, null, null));

        List<Task> running = taskRepository.findByLifecycle(TaskLifecycle.RUNNING);
        assertEquals(2, running.size());
        assertTrue(running.stream().allMatch(task -> task.getLifecycle() == TaskLifecycle.RUNNING));
    }

    @Test
    void shouldPersistAttemptCount() {
        Task task = new Task("req-11", "file-11", "ocr", "{}", TaskLifecycle.RUNNING, "worker-6", 2, 5, null, null);
        Task saved = taskRepository.save(task);

        Task loaded = taskRepository.findByTaskId(saved.getTaskId()).orElseThrow();
        assertEquals(2, loaded.getAttemptCount());
        assertEquals(5, loaded.getMaxRetries());
    }

    @Test
    void shouldPersistAssignedWorker() {
        Task task = new Task("req-12", "file-12", "ocr", "{}", TaskLifecycle.RUNNING, "worker-7", 1, 3, null, null);
        Task saved = taskRepository.save(task);

        Task loaded = taskRepository.findByTaskId(saved.getTaskId()).orElseThrow();
        assertEquals("worker-7", loaded.getAssignedWorkerId());
    }

    @Test
    void shouldPersistErrorDetails() {
        Task task = new Task("req-13", "file-13", "ocr", "{}", TaskLifecycle.FAILED, "worker-8", 1, 3,
                "PROCESSING_ERROR", "Worker crashed");
        Task saved = taskRepository.save(task);

        Task loaded = taskRepository.findByTaskId(saved.getTaskId()).orElseThrow();
        assertEquals("PROCESSING_ERROR", loaded.getErrorCode());
        assertEquals("Worker crashed", loaded.getErrorMessage());
        assertNotNull(loaded.getCreatedAt());
        assertNotNull(loaded.getUpdatedAt());
        assertTrue(loaded.getCreatedAt().isBefore(Instant.now().plusSeconds(1))
                || loaded.getCreatedAt().equals(Instant.now()));
    }
}
