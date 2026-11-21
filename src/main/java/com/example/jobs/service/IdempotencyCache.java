package com.example.jobs.service;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.exception.JobStateConflictException;
import com.example.jobs.web.dto.JobAcceptedResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis front for {@code idempotencyKey}: duplicates are answered from Redis without touching the database.
 *
 * <p>The first request claims the key with {@code SET NX} (a short-lived {@code PENDING} marker), and the accepted
 * response replaces the marker once the job is committed. Concurrent duplicates wait for that response.
 * Redis is an accelerator, not the source of truth: if it is unavailable or has lost the key, the database
 * {@code UNIQUE(idempotency_key)} constraint still rejects the duplicate insert.
 */
@Component
public class IdempotencyCache {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyCache.class);
    private static final String PREFIX = "jobs:idem:";
    private static final String PENDING = "PENDING:";
    private static final long POLL_MS = 20;
    private static final RedisScript<Long> RELEASE_IF_OWNER = RedisScript.of(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
            Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final JobPipelineProperties.Idempotency config;

    public IdempotencyCache(StringRedisTemplate redis, ObjectMapper objectMapper, JobPipelineProperties properties) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.config = properties.idempotency();
    }

    /**
     * Returns the original response if the key was already accepted; empty if the caller now owns the key
     * (or Redis is unavailable) and must create the job.
     */
    public Optional<JobAcceptedResponse> claimOrGetExisting(String key, String claimToken) {
        long deadline = System.currentTimeMillis() + config.awaitMs();
        try {
            while (true) {
                if (Boolean.TRUE.equals(redis.opsForValue()
                        .setIfAbsent(PREFIX + key, PENDING + claimToken, Duration.ofMillis(config.pendingTtlMs())))) {
                    return Optional.empty();
                }
                String value = redis.opsForValue().get(PREFIX + key);
                if (value != null && !value.startsWith(PENDING)) {
                    return Optional.of(asDuplicate(objectMapper.readValue(value, JobAcceptedResponse.class)));
                }
                if (System.currentTimeMillis() >= deadline) {
                    throw new JobStateConflictException(
                            "A request with this idempotencyKey is still being processed; retry shortly");
                }
                Thread.sleep(POLL_MS);
            }
        } catch (DataAccessException | JsonProcessingException ex) {
            log.warn("Idempotency cache unavailable, falling back to the database constraint: {}", ex.getMessage());
            return Optional.empty();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for idempotent request", ex);
        }
    }

    public void complete(String key, JobAcceptedResponse response, long ttlMs) {
        try {
            redis.opsForValue().set(PREFIX + key, objectMapper.writeValueAsString(response), Duration.ofMillis(ttlMs));
        } catch (DataAccessException | JsonProcessingException ex) {
            log.warn("Could not cache idempotent response for key {}: {}", key, ex.getMessage());
        }
    }

    /** Frees a claim whose job was never created so the client can retry immediately. */
    public void release(String key, String claimToken) {
        try {
            redis.execute(RELEASE_IF_OWNER, List.of(PREFIX + key), PENDING + claimToken);
        } catch (DataAccessException ex) {
            log.warn("Could not release idempotency claim for key {}: {}", key, ex.getMessage());
        }
    }

    private static JobAcceptedResponse asDuplicate(JobAcceptedResponse r) {
        return new JobAcceptedResponse(r.jobId(), r.jobType(), r.state(), r.priority(), r.maxRetries(), r.timeoutMs(),
                r.submittedAt(), r.scheduledFor(), r.idempotencyKey(), true, r.metadata(), r.statusUrl());
    }
}
