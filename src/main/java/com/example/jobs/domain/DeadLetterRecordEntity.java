package com.example.jobs.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name = "dead_letter_jobs")
public class DeadLetterRecordEntity {

    @Id
    @Column(name = "id", length = 64, nullable = false, updatable = false)
    private String id;

    @Column(name = "job_id", length = 64, nullable = false)
    private String jobId;

    @Column(name = "outbox_event_id", length = 64, unique = true)
    private String outboxEventId;

    @Column(name = "job_type", length = 100, nullable = false)
    private String jobType;

    @Enumerated(EnumType.STRING)
    @Column(name = "priority", length = 16, nullable = false)
    private JobPriority priority;

    @Column(name = "payload_json", columnDefinition = "TEXT", nullable = false)
    private String payloadJson;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

    @Column(name = "failure_reason", length = 64, nullable = false)
    private String failureReason;

    @Column(name = "error_type", length = 255)
    private String errorType;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "stack_trace", columnDefinition = "TEXT")
    private String stackTrace;

    @Column(name = "original_topic", length = 128, nullable = false)
    private String originalTopic;

    @Column(name = "dead_lettered_at", nullable = false)
    private Instant deadLetteredAt;

    @Column(name = "replayed", nullable = false)
    private boolean replayed;

    @Column(name = "replayed_at")
    private Instant replayedAt;
}
