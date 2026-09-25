package com.example.worker.processor;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessorRegistry {

    private final Map<String, TaskProcessor> processors = new ConcurrentHashMap<>();

    public ProcessorRegistry() {
    }

    public ProcessorRegistry(Map<String, TaskProcessor> initialProcessors) {
        if (initialProcessors != null) {
            initialProcessors.forEach(this::register);
        }
    }

    public void register(String processorType, TaskProcessor processor) {
        if (processorType == null || processorType.isBlank()) {
            throw new IllegalArgumentException("Processor type must not be blank");
        }
        if (processor == null) {
            throw new IllegalArgumentException("Processor must not be null");
        }
        processors.put(normalize(processorType), processor);
    }

    public Optional<TaskProcessor> resolve(String processorType) {
        if (processorType == null || processorType.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(processors.get(normalize(processorType)));
    }

    public boolean supports(String processorType) {
        return resolve(processorType).isPresent();
    }

    public static String normalize(String processorType) {
        return processorType.trim().toUpperCase(Locale.ROOT);
    }
}
