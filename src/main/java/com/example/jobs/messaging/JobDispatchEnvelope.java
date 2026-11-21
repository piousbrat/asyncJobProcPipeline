package com.example.jobs.messaging;

import com.example.jobs.domain.JobPriority;
import java.time.Instant;

public record JobDispatchEnvelope(
        String jobId,
        String jobType,
        JobPriority priority,
        int expectedAttempt,
        int replayGeneration,
        Instant enqueuedAt
) {
}
