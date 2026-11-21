package com.example.jobs.backoff;

import com.example.jobs.config.JobPipelineProperties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Computes non-blocking retry delays using capped exponential backoff with proportional random jitter.
 *
 * <p>For 1-based retry number {@code retryNumber}:
 * <pre>
 *   cappedBase = min(baseIntervalMs * multiplier^(retryNumber - 1), maxIntervalMs)
 *   delay      = uniformRandom([cappedBase * (1 - jitterFactor), cappedBase])
 * </pre>
 */
@Component
public class JitteredExponentialBackoffCalculator {

    private final long baseIntervalMs;
    private final double multiplier;
    private final long maxIntervalMs;
    private final double jitterFactor;
    private final DoubleSupplier randomSource;

    @Autowired
    public JitteredExponentialBackoffCalculator(JobPipelineProperties properties) {
        this(
                properties.retry().baseIntervalMs(),
                properties.retry().multiplier(),
                properties.retry().maxIntervalMs(),
                properties.retry().jitterFactor(),
                () -> ThreadLocalRandom.current().nextDouble());
    }

    public JitteredExponentialBackoffCalculator(
            long baseIntervalMs,
            double multiplier,
            long maxIntervalMs,
            double jitterFactor,
            DoubleSupplier randomSource) {
        if (baseIntervalMs < 1 || maxIntervalMs < 1 || multiplier < 1.0
                || jitterFactor < 0.0 || jitterFactor > 1.0) {
            throw new IllegalArgumentException("Invalid exponential backoff configuration");
        }
        if (randomSource == null) {
            throw new IllegalArgumentException("randomSource must not be null");
        }
        this.baseIntervalMs = baseIntervalMs;
        this.multiplier = multiplier;
        this.maxIntervalMs = maxIntervalMs;
        this.jitterFactor = jitterFactor;
        this.randomSource = randomSource;
    }

    public long computeCappedBaseMs(int retryNumber) {
        if (retryNumber < 1) {
            throw new IllegalArgumentException("retryNumber must be >= 1, got: " + retryNumber);
        }
        double exp = baseIntervalMs * Math.pow(multiplier, retryNumber - 1);
        if (Double.isInfinite(exp) || Double.isNaN(exp) || exp >= maxIntervalMs) {
            return maxIntervalMs;
        }
        return Math.max(1L, Math.round(exp));
    }

    public long computeDelayMs(int retryNumber) {
        long cappedBase = computeCappedBaseMs(retryNumber);
        if (jitterFactor == 0.0) {
            return cappedBase;
        }
        double sample = Math.min(1.0, Math.max(0.0, randomSource.getAsDouble()));
        double minDelay = cappedBase * (1.0 - jitterFactor);
        long jittered = Math.round(minDelay + sample * (cappedBase - minDelay));
        return Math.max(1L, Math.min(maxIntervalMs, jittered));
    }
}
