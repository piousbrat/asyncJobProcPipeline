package com.example.jobs.web.dto;

import com.example.jobs.domain.JobPriority;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public record DeadLetterJobResponse(
        String id,
        String jobId,
        String jobType,
        JobPriority priority,
        JsonNode payload,
        int attemptCount,
        int maxRetries,
        String failureReason,
        String errorType,
        String errorMessage,
        String stackTrace,
        String originalTopic,
        Instant deadLetteredAt,
        boolean replayed,
        Instant replayedAt
) {
}
