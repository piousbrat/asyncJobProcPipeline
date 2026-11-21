package com.example.jobs.worker;

import com.example.jobs.domain.JobPriority;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Map;

public record JobExecutionContext(
        String jobId,
        String jobType,
        JobPriority priority,
        int attemptNumber,
        int maxRetries,
        long timeoutMs,
        String workerId,
        JsonNode payload,
        Map<String, String> metadata,
        Instant submittedAt
) {
}
