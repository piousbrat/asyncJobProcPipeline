package com.example.jobs.worker;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

/**
 * Pluggable contract for async job handlers. Implementations must respect thread interruption
 * so per-attempt timeouts abort work promptly.
 */
public interface JobHandler {

    String jobType();

    /**
     * Checks the payload at submission time so bad input is rejected with {@code 400} instead of dead-lettered.
     * Returns field (relative to the payload, e.g. {@code "numbers[1]"}) to message; empty when valid.
     */
    default Map<String, String> validatePayload(JsonNode payload) {
        return Map.of();
    }

    JsonNode execute(JobExecutionContext context) throws Exception;
}
