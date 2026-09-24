package com.example.scheduler.repository;

import com.example.scheduler.domain.Task;
import com.example.scheduler.domain.TaskLifecycle;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface TaskRepository extends JpaRepository<Task, String> {

    Optional<Task> findByTaskId(String taskId);

    Optional<Task> findByRequestId(String requestId);

    List<Task> findByLifecycleOrderByCreatedAtAsc(TaskLifecycle lifecycle);

    List<Task> findByLifecycle(TaskLifecycle lifecycle);
}
