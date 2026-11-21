package com.example.jobs.messaging;

import com.example.jobs.domain.JobPriority;
import java.time.Instant;

public record DeadLetterEnvelope(
        String outboxEventId,
        String jobId,
        String jobType,
        JobPriority priority,
        String payloadJson,
        int attemptCount,
        int maxRetries,
        String failureReason,
        String errorType,
        String errorMessage,
        String stackTrace,
        String originalTopic,
        Instant failedAt
) {
}
