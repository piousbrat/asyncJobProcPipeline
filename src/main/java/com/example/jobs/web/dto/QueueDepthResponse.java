package com.example.jobs.web.dto;

import com.example.jobs.domain.JobPriority;
import java.util.Map;

/**
 * Backlog snapshot. {@code ready} jobs are QUEUED and due now; {@code delayed} are QUEUED but waiting for
 * {@code scheduledAt}/{@code delayMs} or a retry backoff. {@code outboxPending} is not-yet-published dispatches.
 */
public record QueueDepthResponse(
        Map<JobPriority, Depth> queued,
        long running,
        long outboxPending
) {
    public record Depth(long ready, long delayed) {
    }
}
