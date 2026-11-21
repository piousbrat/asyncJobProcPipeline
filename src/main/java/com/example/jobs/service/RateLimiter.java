package com.example.jobs.service;

import com.example.jobs.config.JobPipelineProperties;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * Cluster-wide sliding-window-log rate limiter: at most {@code limit} requests per client in any rolling
 * {@code windowMs}. One Lua script per request keeps the check-and-record atomic, and Redis {@code TIME}
 * gives every replica the same clock.
 */
@Component
public class RateLimiter {

    public record Decision(boolean allowed, int limit, long remaining, long retryAfterMs) {
    }

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);
    private static final String PREFIX = "jobs:rate:";
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> SLIDING_WINDOW = RedisScript.of("""
            local t = redis.call('TIME')
            local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
            local limit = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
            local count = redis.call('ZCARD', KEYS[1])
            if count < limit then
              redis.call('ZADD', KEYS[1], now, ARGV[3])
              redis.call('PEXPIRE', KEYS[1], window)
              return {1, limit - count - 1, 0}
            end
            local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
            return {0, 0, tonumber(oldest[2]) + window - now}
            """, List.class);

    private final StringRedisTemplate redis;
    private final JobPipelineProperties.RateLimit config;

    public RateLimiter(StringRedisTemplate redis, JobPipelineProperties properties) {
        this.redis = redis;
        this.config = properties.rateLimit();
    }

    /** Fails open when Redis is unreachable: availability of the API wins over strict throttling. */
    public Decision tryAcquire(String clientId) {
        try {
            List<?> r = redis.execute(SLIDING_WINDOW, List.of(PREFIX + clientId),
                    String.valueOf(config.limit()), String.valueOf(config.windowMs()), UUID.randomUUID().toString());
            return new Decision(((Number) r.get(0)).longValue() == 1, config.limit(),
                    ((Number) r.get(1)).longValue(), Math.max(1, ((Number) r.get(2)).longValue()));
        } catch (DataAccessException ex) {
            log.warn("Rate limiter unavailable, allowing request from {}: {}", clientId, ex.getMessage());
            return new Decision(true, config.limit(), config.limit(), 0);
        }
    }
}
