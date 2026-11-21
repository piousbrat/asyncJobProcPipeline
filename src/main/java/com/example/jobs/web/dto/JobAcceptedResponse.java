package com.example.jobs.web.dto;

import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import java.time.Instant;
import java.util.Map;

public record JobAcceptedResponse(
        String jobId,
        String jobType,
        JobStatus state,
        JobPriority priority,
        int maxRetries,
        long timeoutMs,
        Instant submittedAt,
        Instant scheduledFor,
        String idempotencyKey,
        boolean deduplicated,
        Map<String, String> metadata,
        String statusUrl
) {
}
