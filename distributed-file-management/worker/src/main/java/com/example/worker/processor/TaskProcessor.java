package com.example.worker.processor;

import com.example.nimbus.v1.TaskConfiguration;
import com.example.nimbus.v1.TaskResultData;

import java.util.Locale;

@FunctionalInterface
public interface TaskProcessor {

    TaskResultData process(String fileId, TaskConfiguration configuration);

    default String processorType() {
        String simpleName = getClass().getSimpleName();
        if (simpleName.endsWith("Processor")) {
            simpleName = simpleName.substring(0, simpleName.length() - "Processor".length());
        }
        return simpleName.toUpperCase(Locale.ROOT);
    }
}
