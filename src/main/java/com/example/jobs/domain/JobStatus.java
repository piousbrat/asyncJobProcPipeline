package com.example.jobs.domain;

/**
 * Canonical job lifecycle states exposed by {@code GET /api/v1/jobs/{jobId}}.
 *
 * <ul>
 *   <li>{@link #QUEUED}: Accepted for execution (initial submission, deferred schedule, or awaiting a scheduled retry).</li>
 *   <li>{@link #RUNNING}: Atomically claimed by a worker node with an active fencing lease.</li>
 *   <li>{@link #SUCCEEDED}: Completed within timeout; handler JSON result is persisted durably.</li>
 *   <li>{@link #FAILED}: Terminal failure not published to DLQ (e.g. non-DLQ domain failure or pre-DLQ outbox relay).</li>
 *   <li>{@link #DEAD_LETTERED}: Terminal failure published to the Kafka DLQ topic ({@code jobs.dlq}).</li>
 * </ul>
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    DEAD_LETTERED
}
