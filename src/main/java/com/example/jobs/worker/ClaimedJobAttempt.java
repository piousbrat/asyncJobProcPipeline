package com.example.jobs.worker;

import com.example.jobs.domain.JobEntity;
import java.time.Instant;

public record ClaimedJobAttempt(
        JobEntity job,
        String attemptEntityId,
        String leaseToken,
        Instant startedAt
) {
}
