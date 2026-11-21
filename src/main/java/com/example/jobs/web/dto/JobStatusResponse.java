package com.example.jobs.web.dto;

import com.example.jobs.domain.AttemptStatus;
import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public record JobStatusResponse(
        String jobId,
        String jobType,
        JobStatus state,
        JobPriority priority,
        int attemptCount,
        int maxRetries,
        long timeoutMs,
        JsonNode result,
        ErrorDetail lastError,
        Instant submittedAt,
        Instant scheduledFor,
        Instant startedAt,
        Instant finishedAt,
        Instant nextRetryAt,
        String idempotencyKey,
        Map<String, String> metadata,
        int replayCount,
        List<AttemptSummary> attempts
) {
    public JobStatusResponse {
        if (result != null && result.isNull()) {
            result = null;
        }
    }

    public record ErrorDetail(
            String type,
            String message
    ) {
    }

    public record AttemptSummary(
            int attemptNumber,
            int replayGeneration,
            String workerId,
            AttemptStatus status,
            Instant startedAt,
            Instant finishedAt,
            Long durationMs,
            String errorType,
            String errorMessage,
            Boolean retryable
    ) {
    }
}
