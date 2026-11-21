package com.example.jobs.service;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.web.dto.CreateJobRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import org.springframework.stereotype.Component;

/**
 * Every submission gets an idempotency key, in order of preference:
 * <ol>
 *   <li>{@code EXPLICIT}: the client's {@code idempotencyKey}, used as-is and kept for {@code ttl-ms} in Redis.</li>
 *   <li>{@code EVENT}: {@code evt:sha256(client, X-Event-Id)}. An upstream event id is a stable identity, so it
 *       deduplicates like an explicit key.</li>
 *   <li>{@code FINGERPRINT}: {@code fp:sha256(client, canonical request body)}. Identical submissions from one client
 *       within {@code fingerprint-window-ms} collapse into one job. Redis holds the key for exactly that window from the
 *       first submission. The database key adds the window bucket ({@code :<epochMs / window>}), so the permanent
 *       {@code UNIQUE} constraint only collapses repeats inside one bucket when Redis is unavailable.</li>
 * </ol>
 */
@Component
public class IdempotencyKeyResolver {

    public enum Source { EXPLICIT, EVENT, FINGERPRINT }

    /** {@code cacheKey} is used in Redis, {@code dbKey} is stored in {@code jobs.idempotency_key}. */
    public record ResolvedKey(String cacheKey, String dbKey, long cacheTtlMs, Source source) {
    }

    private final ObjectMapper canonicalMapper;
    private final JobPipelineProperties.Idempotency config;

    public IdempotencyKeyResolver(ObjectMapper objectMapper, JobPipelineProperties properties) {
        this.canonicalMapper = objectMapper.copy().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        this.config = properties.idempotency();
    }

    /** Returns null only when no explicit key or event id was given and fingerprinting is disabled (window 0). */
    public ResolvedKey resolve(CreateJobRequest request, String clientId, String eventId, Instant now) {
        String explicit = trimToNull(request.idempotencyKey());
        if (explicit != null) {
            return new ResolvedKey(explicit, explicit, config.ttlMs(), Source.EXPLICIT);
        }
        String event = trimToNull(eventId);
        if (event != null) {
            String key = "evt:" + sha256(clientId + "\n" + event);
            return new ResolvedKey(key, key, config.ttlMs(), Source.EVENT);
        }
        long window = config.fingerprintWindowMs();
        if (window <= 0) {
            return null;
        }
        String fingerprint = "fp:" + sha256(clientId + "\n" + canonicalJson(request));
        return new ResolvedKey(fingerprint, fingerprint + ":" + now.toEpochMilli() / window, window, Source.FINGERPRINT);
    }

    /** Key-order independent: {@code {"a":1,"b":2}} and {@code {"b":2,"a":1}} produce the same fingerprint. */
    String canonicalJson(CreateJobRequest request) {
        try {
            return canonicalMapper.writeValueAsString(canonicalMapper.convertValue(request, Object.class));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to fingerprint request", e);
        }
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String trimToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
