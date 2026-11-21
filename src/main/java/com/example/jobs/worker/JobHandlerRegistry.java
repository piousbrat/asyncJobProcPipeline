package com.example.jobs.worker;

import com.example.jobs.exception.RequestValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

    private static final Set<String> BOOLEAN_DIRECTIVES = Set.of(
            "simulateTerminalFailure", "simulateNonRetryable", "simulateInvalid", "simulateAlwaysFail");
    private static final Set<String> COUNT_DIRECTIVES = Set.of("simulateDelayMs", "simulateFailures");

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

    public List<String> supportedTypes() {
        return List.copyOf(handlers.keySet());
    }

    /**
     * Rejects an unknown job type or a payload its handler would refuse, reporting every problem at once.
     * {@code fieldPrefix} is where the job sits in the request body: {@code ""} for a job, {@code "job."} for a schedule.
     */
    public void validate(String fieldPrefix, String jobType, JsonNode payload) {
        Map<String, String> errors = new LinkedHashMap<>();
        JobHandler handler = jobType == null ? null : handlers.get(jobType.trim());
        if (handler == null) {
            errors.put(fieldPrefix + "jobType", "Unsupported jobType '" + jobType
                    + "'; supported types: " + String.join(", ", handlers.keySet()));
        }
        if (payload == null || !payload.isObject()) {
            errors.put(fieldPrefix + "payload", "payload must be a JSON object");
        } else {
            Map<String, String> payloadErrors = new LinkedHashMap<>(directiveErrors(payload));
            if (handler != null) {
                payloadErrors.putAll(handler.validatePayload(payload));
            }
            payloadErrors.forEach((field, message) -> errors.put(fieldPrefix + "payload." + field, message));
        }
        if (!errors.isEmpty()) {
            throw new RequestValidationException(errors);
        }
    }

    /** The {@code simulate*} directives apply to every job type, see {@code JobExecutionEngine}. */
    private static Map<String, String> directiveErrors(JsonNode payload) {
        Map<String, String> errors = new LinkedHashMap<>();
        BOOLEAN_DIRECTIVES.stream().sorted().filter(payload::has)
                .filter(name -> !payload.get(name).isBoolean())
                .forEach(name -> errors.put(name, "must be a boolean"));
        COUNT_DIRECTIVES.stream().sorted().filter(payload::has)
                .filter(name -> !payload.get(name).canConvertToLong() || !payload.get(name).isIntegralNumber()
                        || payload.get(name).asLong() < 0)
                .forEach(name -> errors.put(name, "must be a non-negative integer"));
        return errors;
    }
}
