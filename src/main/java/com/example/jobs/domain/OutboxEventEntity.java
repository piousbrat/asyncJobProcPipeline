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
@Table(name = "outbox_events")
public class OutboxEventEntity {

    @Id
    @Column(name = "id", length = 64, nullable = false, updatable = false)
    private String id;

    @Column(name = "job_id", length = 64, nullable = false)
    private String jobId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", length = 32, nullable = false)
    private OutboxEventType eventType;

    @Column(name = "topic", length = 128, nullable = false)
    private String topic;

    @Column(name = "partition_key", length = 128, nullable = false)
    private String partitionKey;

    @Column(name = "priority_rank", nullable = false)
    private int priorityRank;

    @Column(name = "payload_json", columnDefinition = "TEXT", nullable = false)
    private String payloadJson;

    @Column(name = "headers_json", columnDefinition = "TEXT")
    private String headersJson;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 32, nullable = false)
    private OutboxStatus status;

    @Column(name = "available_at", nullable = false)
    private Instant availableAt;

    @Column(name = "locked_by", length = 128)
    private String lockedBy;

    @Column(name = "lock_expires_at")
    private Instant lockExpiresAt;

    @Column(name = "publish_attempts", nullable = false)
    private int publishAttempts;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;
}
