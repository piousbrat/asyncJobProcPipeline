package com.example.jobs.backoff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

class JitteredExponentialBackoffCalculatorTest {

    @Test
    void computesDeterministicExponentialCurveAndCapWhenJitterDisabled() {
        JitteredExponentialBackoffCalculator calculator =
                new JitteredExponentialBackoffCalculator(100L, 2.0, 500L, 0.0, () -> 0.5);

        assertThat(calculator.computeDelayMs(1)).isEqualTo(100L);
        assertThat(calculator.computeDelayMs(2)).isEqualTo(200L);
        assertThat(calculator.computeDelayMs(3)).isEqualTo(400L);
        assertThat(calculator.computeDelayMs(4)).isEqualTo(500L);
        assertThat(calculator.computeDelayMs(50)).isEqualTo(500L);
    }

    @Test
    void appliesJitterStrictlyWithinLowerAndUpperBounds() {
        JitteredExponentialBackoffCalculator minSample =
                new JitteredExponentialBackoffCalculator(1000L, 2.0, 10_000L, 0.4, () -> 0.0);
        JitteredExponentialBackoffCalculator maxSample =
                new JitteredExponentialBackoffCalculator(1000L, 2.0, 10_000L, 0.4, () -> 1.0);

        // Retry #2 base = 2000ms -> [1200ms, 2000ms]
        assertThat(minSample.computeDelayMs(2)).isEqualTo(1200L);
        assertThat(maxSample.computeDelayMs(2)).isEqualTo(2000L);

        JitteredExponentialBackoffCalculator randomCalc = new JitteredExponentialBackoffCalculator(
                1000L, 2.0, 10_000L, 0.5, () -> ThreadLocalRandom.current().nextDouble());

        Set<Long> distinctValues = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            long delay = randomCalc.computeDelayMs(3); // base = 4000 -> [2000, 4000]
            assertThat(delay).isBetween(2000L, 4000L);
            distinctValues.add(delay);
        }
        assertThat(distinctValues.size()).isGreaterThan(10);
    }

    @Test
    void fixedPolicyRetriesThreeTimesWithJitteredDelaysGrowingBy1Point01() {
        assertThat(RetryPolicy.MAX_RETRIES).isEqualTo(3);
        JitteredExponentialBackoffCalculator calculator = new JitteredExponentialBackoffCalculator();

        for (int retry = 1; retry <= RetryPolicy.MAX_RETRIES; retry++) {
            long base = Math.round(1000 * Math.pow(1.01, retry - 1));
            assertThat(calculator.computeCappedBaseMs(retry)).isEqualTo(base);
            for (int i = 0; i < 50; i++) {
                assertThat(calculator.computeDelayMs(retry)).isBetween(Math.round(base * 0.7), base);
            }
        }
    }

    @Test
    void rejectsInvalidConfigurationAndRetryNumbers() {
        assertThatThrownBy(() -> new JitteredExponentialBackoffCalculator(0L, 2.0, 1000L, 0.2, () -> 0.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JitteredExponentialBackoffCalculator(100L, 0.5, 1000L, 0.2, () -> 0.5))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JitteredExponentialBackoffCalculator(100L, 2.0, 1000L, 1.5, () -> 0.5))
                .isInstanceOf(IllegalArgumentException.class);

        JitteredExponentialBackoffCalculator calc =
                new JitteredExponentialBackoffCalculator(100L, 2.0, 1000L, 0.2, () -> 0.5);
        assertThatThrownBy(() -> calc.computeDelayMs(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
