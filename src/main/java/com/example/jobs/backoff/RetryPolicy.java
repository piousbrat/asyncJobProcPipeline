package com.example.jobs.backoff;

/**
 * Fixed retry policy: a failed attempt is retried up to {@link #MAX_RETRIES} times (so at most 4 attempts), waiting
 * {@code min(BASE_INTERVAL_MS * MULTIPLIER^(retry-1), MAX_INTERVAL_MS)} reduced by up to {@link #JITTER_FACTOR}.
 */
public final class RetryPolicy {

    public static final int MAX_RETRIES = 3;
    public static final long BASE_INTERVAL_MS = 1000L;
    public static final double MULTIPLIER = 1.01;
    public static final long MAX_INTERVAL_MS = 60_000L;
    public static final double JITTER_FACTOR = 0.3;

    private RetryPolicy() {
    }
}
