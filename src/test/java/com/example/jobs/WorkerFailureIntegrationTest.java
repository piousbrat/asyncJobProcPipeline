package com.example.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.jobs.domain.AttemptStatus;
import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.messaging.JobDispatchEnvelope;
import com.example.jobs.web.dto.DeadLetterJobResponse;
import com.example.jobs.web.dto.JobAcceptedResponse;
import com.example.jobs.web.dto.JobStatusResponse;
import com.example.jobs.worker.ClaimedJobAttempt;
import com.example.jobs.worker.JobHandler;
import com.example.jobs.worker.WorkerStateTxService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Submits jobs over HTTP, injects worker failures (handler errors, hangs, timeouts, a worker that dies mid-attempt,
 * duplicate Kafka deliveries) and asserts each job converges to the expected final state.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.datasource.url=jdbc:h2:mem:failuredb;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;"
                + "DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1")
@EmbeddedKafka(partitions = 3, topics = {"jobs.high", "jobs.normal", "jobs.low", "jobs.dlq"})
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class WorkerFailureIntegrationTest {

    @TestConfiguration
    static class FailureHandlers {

        static final Map<String, AtomicInteger> RUNS = new ConcurrentHashMap<>();

        /** Throws an unexpected runtime error (not a simulated one) until attempt {@code failUntil}. */
        @Bean
        JobHandler flakyRuntimeHandler(ObjectMapper mapper) {
            return handler("flaky.runtime", mapper, (ctx, out) -> {
                if (ctx.attemptNumber() <= ctx.payload().path("failUntil").asInt()) {
                    throw new IllegalStateException("boom on attempt " + ctx.attemptNumber());
                }
                out.put("attempt", ctx.attemptNumber());
            });
        }

        /** Counts real executions per tag, so duplicate execution is observable. */
        @Bean
        JobHandler slowCountingHandler(ObjectMapper mapper) {
            return handler("slow.count", mapper, (ctx, out) -> {
                String tag = ctx.payload().path("tag").asText();
                int runs = RUNS.computeIfAbsent(tag, k -> new AtomicInteger()).incrementAndGet();
                Thread.sleep(ctx.payload().path("workMs").asLong());
                out.put("runs", runs);
            });
        }

        /** Busy-waits ignoring interruption, like a handler stuck in non-interruptible I/O. */
        @Bean
        JobHandler stubbornHandler(ObjectMapper mapper) {
            return handler("stubborn", mapper, (ctx, out) -> {
                long until = System.nanoTime() + ctx.payload().path("workMs").asLong() * 1_000_000;
                while (System.nanoTime() < until) {
                    Thread.onSpinWait();
                }
                out.put("done", true);
            });
        }

        private static JobHandler handler(String type, ObjectMapper mapper, Body body) {
            return new JobHandler() {
                @Override
                public String jobType() {
                    return type;
                }

                @Override
                public com.fasterxml.jackson.databind.JsonNode execute(
                        com.example.jobs.worker.JobExecutionContext ctx) throws Exception {
                    ObjectNode out = mapper.createObjectNode();
                    body.run(ctx, out);
                    return out;
                }
            };
        }

        interface Body {
            void run(com.example.jobs.worker.JobExecutionContext ctx, ObjectNode out) throws Exception;
        }
    }

    private record Scenario(
            Map<String, Object> request, JobStatus state, int attempts, String errorType, AttemptStatus lastAttempt) {
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @MockitoSpyBean
    private WorkerStateTxService workerStateTxService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void mixedWorkerFailuresConvergeToExpectedFinalStatesAndOnlyDeadLetteredJobsReachTheDlq() {
        Map<String, Scenario> scenarios = new LinkedHashMap<>();
        scenarios.put("transient failures then success", new Scenario(
                job("echo", Map.of("message", "m", "simulateFailures", 2), 3, 2000),
                JobStatus.SUCCEEDED, 3, "SimulatedTransientFailureException", AttemptStatus.SUCCEEDED));
        scenarios.put("unexpected runtime error then success", new Scenario(
                job("flaky.runtime", Map.of("failUntil", 1), 2, 2000),
                JobStatus.SUCCEEDED, 2, "IllegalStateException", AttemptStatus.SUCCEEDED));
        scenarios.put("healthy", new Scenario(
                job("sum", Map.of("numbers", List.of(1, 2, 3)), 3, 2000),
                JobStatus.SUCCEEDED, 1, null, AttemptStatus.SUCCEEDED));
        scenarios.put("retries exhausted", new Scenario(
                job("echo", Map.of("message", "m", "simulateAlwaysFail", true), 2, 2000),
                JobStatus.DEAD_LETTERED, 3, "SimulatedTransientFailureException", AttemptStatus.FAILED));
        scenarios.put("default policy retries 3 times", new Scenario(
                withoutMaxRetries(job("echo", Map.of("message", "m", "simulateAlwaysFail", true), 0, 2000)),
                JobStatus.DEAD_LETTERED, 4, "SimulatedTransientFailureException", AttemptStatus.FAILED));
        scenarios.put("maxRetries=0 fails once", new Scenario(
                job("echo", Map.of("message", "m", "simulateFailures", 1), 0, 2000),
                JobStatus.DEAD_LETTERED, 1, "SimulatedTransientFailureException", AttemptStatus.FAILED));
        scenarios.put("hung handler times out every attempt", new Scenario(
                job("simulate", Map.of("simulateDelayMs", 10_000), 1, 100),
                JobStatus.DEAD_LETTERED, 2, "JobAttemptTimeoutException", AttemptStatus.TIMED_OUT));
        scenarios.put("non-retryable skips retries", new Scenario(
                job("sum", Map.of("numbers", List.of(1), "simulateInvalid", true), 5, 2000),
                JobStatus.DEAD_LETTERED, 1, "InvalidPayloadException", AttemptStatus.FAILED));
        scenarios.put("terminal failure bypasses the DLQ", new Scenario(
                job("simulate", Map.of("simulateTerminalFailure", true), 4, 2000),
                JobStatus.FAILED, 1, "TerminalJobFailureException", AttemptStatus.FAILED));

        Map<String, String> idsByName = new LinkedHashMap<>();
        scenarios.forEach((name, s) -> idsByName.put(name, post(s.request()).getBody().jobId()));

        scenarios.forEach((name, expected) -> await().alias(name).atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            JobStatusResponse actual = getJob(idsByName.get(name));
            assertThat(actual.state()).isEqualTo(expected.state());
            assertThat(actual.attemptCount()).isEqualTo(expected.attempts());
            assertThat(actual.attempts()).hasSize(expected.attempts());
            assertThat(actual.attempts().get(expected.attempts() - 1).status()).isEqualTo(expected.lastAttempt());
            if (expected.errorType() == null) {
                assertThat(actual.lastError()).isNull();
            } else {
                assertThat(actual.lastError().type()).endsWith(expected.errorType());
            }
            assertThat(actual.result() != null).isEqualTo(expected.state() == JobStatus.SUCCEEDED);
            assertThat(actual.nextRetryAt()).isNull();
        }));

        Set<String> deadLetteredIds = scenarios.entrySet().stream()
                .filter(e -> e.getValue().state() == JobStatus.DEAD_LETTERED)
                .map(e -> idsByName.get(e.getKey()))
                .collect(Collectors.toSet());
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Set<String> inDlq = dlqJobIds();
            assertThat(inDlq).containsAll(deadLetteredIds);
            idsByName.values().stream().filter(id -> !deadLetteredIds.contains(id))
                    .forEach(id -> assertThat(inDlq).doesNotContain(id));
        });
    }

    @Test
    void bulkJobsWithRandomWorkerFailuresAllConvergeWithExactAttemptCounts() {
        JobPriority[] priorities = JobPriority.values();
        int jobs = 30;
        Map<String, Integer> expectedAttempts = new LinkedHashMap<>();
        for (int i = 0; i < jobs; i++) {
            int failures = i % 4;
            Map<String, Object> request = new LinkedHashMap<>(job(
                    "echo", Map.of("message", "bulk-" + i, "simulateFailures", failures), 3, 2000));
            request.put("priority", priorities[i % priorities.length].name());
            expectedAttempts.put(post(request).getBody().jobId(), failures + 1);
        }

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                expectedAttempts.forEach((id, attempts) -> {
                    JobStatusResponse status = getJob(id);
                    assertThat(status.state()).isEqualTo(JobStatus.SUCCEEDED);
                    assertThat(status.attemptCount()).isEqualTo(attempts);
                    assertThat(status.result().get("attempt").asInt()).isEqualTo(attempts);
                    assertThat(status.lastError() != null).isEqualTo(attempts > 1);
                }));
    }

    @Test
    void jobsClaimedByAWorkerThatDiesAreRecoveredAfterLeaseExpiryAndZombieWritesAreFenced() {
        // A worker "dies" right after claiming: it never heartbeats, completes or fails the attempt.
        // Delayed submission keeps the original dispatch out of Kafka so only the test claims attempt 1.
        String recoverable = post(delayed(job("echo", Map.of("message", "survivor"), 2, 200))).getBody().jobId();
        String exhausted = post(delayed(job("echo", Map.of("message", "doomed"), 0, 200))).getBody().jobId();

        double recoveredBefore = counter("jobs.lease.recovered");
        double fencedBefore = counter("jobs.execution.fenced", "outcome", "SUCCEEDED");
        ClaimedJobAttempt zombie = claimAsDeadWorker(recoverable);
        claimAsDeadWorker(exhausted);
        assertThat(getJob(recoverable).state()).isEqualTo(JobStatus.RUNNING);

        // Stale-lease recovery (no manual intervention): attempt 2 is re-dispatched and succeeds.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            JobStatusResponse status = getJob(recoverable);
            assertThat(status.state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(status.attemptCount()).isEqualTo(2);
            assertThat(status.result().get("echo").asText()).isEqualTo("survivor");
            assertThat(status.attempts().get(0).status()).isEqualTo(AttemptStatus.TIMED_OUT);
            assertThat(status.attempts().get(0).errorMessage()).contains("lease expired");
            assertThat(status.attempts().get(1).status()).isEqualTo(AttemptStatus.SUCCEEDED);
        });

        // With no retries left, the dead worker's job goes straight to the DLQ.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            JobStatusResponse status = getJob(exhausted);
            assertThat(status.state()).isEqualTo(JobStatus.DEAD_LETTERED);
            assertThat(status.attemptCount()).isEqualTo(1);
            assertThat(status.lastError().message()).contains("lease expired");
            assertThat(dlqJobIds()).contains(exhausted);
        });

        // The zombie wakes up and tries to report: its stale lease token is rejected and nothing changes.
        assertThat(workerStateTxService.completeSuccess(zombie, objectMapper.createObjectNode().put("echo", "zombie")))
                .isFalse();
        JobStatusResponse afterZombie = getJob(recoverable);
        assertThat(afterZombie.state()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(afterZombie.result().get("echo").asText()).isEqualTo("survivor");

        // The duplicate execution is detectable: per job on the attempt row, and fleet-wide via metrics.
        assertThat(afterZombie.attempts().get(0).errorMessage())
                .contains("late SUCCEEDED result discarded", "possible duplicate execution");
        assertThat(counter("jobs.execution.fenced", "outcome", "SUCCEEDED")).isEqualTo(fencedBefore + 1);
        assertThat(counter("jobs.lease.recovered")).isGreaterThanOrEqualTo(recoveredBefore + 2);
    }

    @Test
    void handlerThatIgnoresTheTimeoutAndFinishesLateIsCounted() {
        double before = counter("jobs.execution.completed_after_timeout");
        String jobId = post(job("stubborn", Map.of("workMs", 700, "tag", UUID.randomUUID().toString()), 0, 200))
                .getBody().jobId();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(getJob(jobId).state()).isEqualTo(JobStatus.DEAD_LETTERED);
            assertThat(counter("jobs.execution.completed_after_timeout")).isEqualTo(before + 1);
        });
        assertThat(getJob(jobId).attempts()).singleElement()
                .satisfies(a -> assertThat(a.status()).isEqualTo(AttemptStatus.TIMED_OUT));
    }

    @Test
    void duplicateKafkaDeliveriesWhileAnAttemptIsRunningNeverExecuteTheJobTwice() throws Exception {
        String tag = "dup-" + UUID.randomUUID();
        String jobId = post(job("slow.count", Map.of("tag", tag, "workMs", 800), 3, 5000)).getBody().jobId();
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(20)).untilAsserted(() ->
                assertThat(getJob(jobId).state()).isEqualTo(JobStatus.RUNNING));

        double skippedBefore = counter("jobs.dispatch.duplicates_skipped", "reason", "stale");
        String duplicate = objectMapper.writeValueAsString(
                new JobDispatchEnvelope(jobId, "slow.count", JobPriority.NORMAL, 1, 0, Instant.now()));
        for (int i = 0; i < 5; i++) {
            kafkaTemplate.send("jobs.normal", jobId, duplicate).get();
        }

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(getJob(jobId).state()).isEqualTo(JobStatus.SUCCEEDED));
        JobStatusResponse done = getJob(jobId);
        assertThat(done.attemptCount()).isEqualTo(1);
        assertThat(done.attempts()).hasSize(1);
        assertThat(done.result().get("runs").asInt()).isEqualTo(1);
        assertThat(FailureHandlers.RUNS.get(tag).get()).isEqualTo(1);
        assertThat(counter("jobs.dispatch.duplicates_skipped", "reason", "stale")).isGreaterThanOrEqualTo(skippedBefore + 5);
    }

    @Test
    void dispatchIsNotLostWhenTheDatabaseFailsWhileClaiming() {
        // More consecutive claim failures than a bounded in-container retry would tolerate.
        org.mockito.Mockito.doThrow(new IllegalStateException("db down"))
                .doThrow(new IllegalStateException("db down"))
                .doThrow(new IllegalStateException("db down"))
                .doCallRealMethod()
                .when(workerStateTxService).tryClaimAttempt(org.mockito.ArgumentMatchers.any());
        try {
            String jobId = post(job("echo", Map.of("message", "survives-db-outage"), 3, 2000)).getBody().jobId();

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                JobStatusResponse status = getJob(jobId);
                assertThat(status.state()).isEqualTo(JobStatus.SUCCEEDED);
                assertThat(status.attemptCount()).isEqualTo(1);
                assertThat(status.result().get("echo").asText()).isEqualTo("survives-db-outage");
            });
        } finally {
            org.mockito.Mockito.reset(workerStateTxService);
        }
    }

    private double counter(String name, String... tags) {
        Counter c = meterRegistry.find(name).tags(tags).counter();
        return c == null ? 0 : c.count();
    }

    private ClaimedJobAttempt claimAsDeadWorker(String jobId) {
        return workerStateTxService.tryClaimAttempt(
                new JobDispatchEnvelope(jobId, "echo", JobPriority.NORMAL, 1, 0, Instant.now())).orElseThrow();
    }

    private Map<String, Object> job(String type, Map<String, Object> payload, int maxRetries, long timeoutMs) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("jobType", type);
        request.put("payload", payload);
        request.put("maxRetries", maxRetries);
        request.put("timeoutMs", timeoutMs);
        return request;
    }

    private Map<String, Object> withoutMaxRetries(Map<String, Object> request) {
        Map<String, Object> copy = new LinkedHashMap<>(request);
        copy.remove("maxRetries");
        return copy;
    }

    private Map<String, Object> delayed(Map<String, Object> request) {
        Map<String, Object> copy = new LinkedHashMap<>(request);
        copy.put("delayMs", 60_000);
        return copy;
    }

    private ResponseEntity<JobAcceptedResponse> post(Map<String, Object> request) {
        ResponseEntity<JobAcceptedResponse> response =
                restTemplate.postForEntity("/api/v1/jobs", request, JobAcceptedResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return response;
    }

    private JobStatusResponse getJob(String jobId) {
        ResponseEntity<JobStatusResponse> response =
                restTemplate.getForEntity("/api/v1/jobs/" + jobId, JobStatusResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private Set<String> dlqJobIds() {
        List<DeadLetterJobResponse> all = restTemplate.exchange(
                "/api/v1/dlq", HttpMethod.GET, null, new ParameterizedTypeReference<List<DeadLetterJobResponse>>() {})
                .getBody();
        return all.stream().map(DeadLetterJobResponse::jobId).collect(Collectors.toSet());
    }
}
