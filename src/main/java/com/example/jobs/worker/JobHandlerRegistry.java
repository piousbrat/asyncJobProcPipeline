package com.example.jobs.worker;

import com.example.jobs.exception.RequestValidationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * The job types this deployment can execute. Submissions are validated against it, so every node must ship the same
 * handlers (roll out a new handler before clients start submitting its type).
 */
@Component
public class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers;

    public JobHandlerRegistry(List<JobHandler> handlers) {
        this.handlers = handlers.stream()
                .collect(Collectors.toMap(JobHandler::jobType, Function.identity(), (a, b) -> {
                    throw new IllegalStateException("Duplicate handler for jobType " + a.jobType());
                }, TreeMap::new));
    }

    public Optional<JobHandler> find(String jobType) {
        return Optional.ofNullable(handlers.get(jobType));
    }

    public boolean supports(String jobType) {
        return jobType != null && handlers.containsKey(jobType.trim());
    }

    public void requireSupported(String field, String jobType) {
        if (!supports(jobType)) {
            throw new RequestValidationException(field, "Unsupported jobType '" + jobType
                    + "'; supported types: " + String.join(", ", handlers.keySet()));
        }
    }

    public List<String> supportedTypes() {
        return List.copyOf(handlers.keySet());
    }
}
