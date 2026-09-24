package com.example.gateway;

import com.example.nimbus.v1.SchedulerSubmitTaskRequest;
import com.example.nimbus.v1.SchedulerSubmitTaskResponse;

public interface SchedulerClient {
    SchedulerSubmitTaskResponse submitTask(SchedulerSubmitTaskRequest request);
}
