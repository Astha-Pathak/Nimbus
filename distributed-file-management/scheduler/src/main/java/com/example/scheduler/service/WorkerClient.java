package com.example.scheduler.service;

import com.example.nimbus.v1.WorkerAssignTaskResponse;
import com.example.nimbus.v1.WorkerTaskAssignment;
import com.example.scheduler.domain.WorkerInfo;

public interface WorkerClient {
    WorkerAssignTaskResponse assignTask(WorkerInfo worker, WorkerTaskAssignment assignment);
}
