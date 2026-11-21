package com.example.jobs.worker;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Pluggable contract for async job handlers. Implementations must respect thread interruption
 * so per-attempt timeouts abort work promptly.
 */
public interface JobHandler {

    String jobType();

    JsonNode execute(JobExecutionContext context) throws Exception;
}
