package com.example.worker;

import com.example.worker.processor.CompressionProcessor;
import com.example.worker.processor.ChecksumProcessor;
import com.example.worker.processor.ProcessorRegistry;
import com.example.worker.service.WorkerServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkerConfig {

    @Value("${scheduler.host:localhost}")
    private String schedulerHost;

    @Value("${scheduler.port:9090}")
    private int schedulerPort;

    @Value("${worker.heartbeat.interval-ms:5000}")
    private long heartbeatIntervalMs;

    @Bean
    public ProcessorRegistry processorRegistry() {
        ProcessorRegistry registry = new ProcessorRegistry();
        registry.register("CHECKSUM", new ChecksumProcessor());
        registry.register("COMPRESSION", new CompressionProcessor());
        return registry;
    }

    @Bean
    public WorkerServiceImpl workerServiceImpl(ProcessorRegistry registry) {
        return new WorkerServiceImpl(registry, "worker-local", "localhost", schedulerHost, schedulerPort, heartbeatIntervalMs);
    }
}