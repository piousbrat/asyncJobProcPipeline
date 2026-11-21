package com.example.jobs.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.jobs")
public record JobPipelineProperties(
        @DefaultValue("worker-default") @NotBlank String workerId,
        @DefaultValue @Valid Defaults defaults,
        @DefaultValue @Valid Topics topics,
        @DefaultValue @Valid Consumer consumer,
        @DefaultValue @Valid Outbox outbox,
        @DefaultValue @Valid Worker worker,
        @DefaultValue @Valid Schedules schedules,
        @DefaultValue @Valid Idempotency idempotency,
        @DefaultValue @Valid RateLimit rateLimit) {

    public record Defaults(@DefaultValue("10000") @Min(10) long timeoutMs) {
    }

    public record Topics(
            @DefaultValue("jobs.high") @NotBlank String high,
            @DefaultValue("jobs.normal") @NotBlank String normal,
            @DefaultValue("jobs.low") @NotBlank String low,
            @DefaultValue("jobs.dlq") @NotBlank String dlq,
            @DefaultValue @Valid Partitions partitions,
            @DefaultValue("1") @Min(1) short replicationFactor) {
    }

    public record Partitions(
            @DefaultValue("6") @Min(1) int high,
            @DefaultValue("3") @Min(1) int normal,
            @DefaultValue("2") @Min(1) int low,
            @DefaultValue("3") @Min(1) int dlq) {
    }

    public record Consumer(
            @DefaultValue("4") @Min(1) int highConcurrency,
            @DefaultValue("2") @Min(1) int normalConcurrency,
            @DefaultValue("1") @Min(1) int lowConcurrency,
            @DefaultValue("1") @Min(1) int dlqConcurrency,
            @DefaultValue("10") @Min(1) int highMaxPollRecords,
            @DefaultValue("5") @Min(1) int normalMaxPollRecords,
            @DefaultValue("2") @Min(1) int lowMaxPollRecords) {
    }

    public record Outbox(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("200") @Min(10) long pollIntervalMs,
            @DefaultValue("50") @Min(1) int batchSize,
            @DefaultValue("30000") @Min(100) long lockDurationMs,
            @DefaultValue("5000") @Min(100) long sendTimeoutMs) {
    }

    public record Worker(
            @DefaultValue("16") @Min(1) int executorPoolSize,
            @DefaultValue("5000") @Min(100) long leaseGraceMs,
            @DefaultValue("2000") @Min(100) long heartbeatIntervalMs,
            @DefaultValue("2000") @Min(50) long staleRecoveryIntervalMs,
            @DefaultValue("25") @Min(1) int staleRecoveryBatchSize) {
    }

    public record Schedules(
            @DefaultValue("1000") @Min(50) long pollIntervalMs,
            @DefaultValue("100") @Min(1) int batchSize) {
    }

    public record Idempotency(
            @DefaultValue("86400000") @Min(1000) long ttlMs,
            @DefaultValue("30000") @Min(100) long pendingTtlMs,
            @DefaultValue("2000") @Min(0) long awaitMs,
            @DefaultValue("600000") @Min(0) long fingerprintWindowMs) {
    }

    public record RateLimit(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("10") @Min(1) int limit,
            @DefaultValue("3000") @Min(1) long windowMs) {
    }
}
