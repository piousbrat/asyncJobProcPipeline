package com.example.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.jobs.domain.AttemptStatus;
import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.domain.OutboxEventEntity;
import com.example.jobs.messaging.JobDispatchEnvelope;
import com.example.jobs.outbox.OutboxTxService;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.OutboxEventRepository;
import com.example.jobs.web.dto.DeadLetterJobResponse;
import com.example.jobs.web.dto.JobAcceptedResponse;
import com.example.jobs.web.dto.JobStatusResponse;
import com.example.jobs.worker.ClaimedJobAttempt;
import com.example.jobs.worker.JobHandler;
import com.example.jobs.worker.WorkerStateTxService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(
        partitions = 3,
        topics = {"jobs.high", "jobs.normal", "jobs.low", "jobs.dlq"})
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DistributedPipelineIntegrationTest {

    @TestConfiguration
    static class CountingHandlerConfig {

        static final Map<String, AtomicInteger> INVOCATIONS = new ConcurrentHashMap<>();
        static final List<String> COMPLETION_ORDER = Collections.synchronizedList(new ArrayList<>());

        @Bean
        JobHandler countingIdempotentJobHandler(ObjectMapper objectMapper) {
            return new JobHandler() {
                @Override
                public String jobType() {
                    return "counting.work";
                }

                @Override
                public com.fasterxml.jackson.databind.JsonNode execute(
                        com.example.jobs.worker.JobExecutionContext context) throws Exception {
                    String tag = context.payload().path("tag").asText(context.jobId());
                    long workMs = context.payload().path("workMs").asLong(0L);
                    if (workMs > 0) {
                        Thread.sleep(workMs);
                    }
                    int count = INVOCATIONS
                            .computeIfAbsent(tag, k -> new AtomicInteger(0))
                            .incrementAndGet();
                    COMPLETION_ORDER.add(context.priority().name() + ":" + tag);

                    ObjectNode res = objectMapper.createObjectNode();
                    res.put("tag", tag);
                    res.put("invocations", count);
                    return res;
                }
            };
        }
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxTxService outboxTxService;

    @Autowired
    private WorkerStateTxService workerStateTxService;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void immediateSubmissionReturns202AndPersistsDurableJsonResultOnSuccess() {
        Map<String, Object> body = Map.of(
                "jobType", "sum",
                "priority", "NORMAL",
                "payload", Map.of("numbers", List.of(10, 20, 30, 40)),
                "metadata", Map.of("tenant", "acme", "traceId", "tr-001"));

        ResponseEntity<JobAcceptedResponse> accepted = postJob(body);
        assertThat(accepted.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(accepted.getHeaders().getLocation()).isNotNull();

        JobAcceptedResponse acc = accepted.getBody();
        assertThat(acc).isNotNull();
        assertThat(acc.jobId()).isNotBlank();
        assertThat(acc.state()).isEqualTo(JobStatus.QUEUED);
        assertThat(acc.deduplicated()).isFalse();
        assertThat(acc.metadata()).containsEntry("tenant", "acme");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse status = getJob(acc.jobId());
            assertThat(status.state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(status.attemptCount()).isEqualTo(1);
            assertThat(status.lastError()).isNull();
            assertThat(status.result()).isNotNull();
            assertThat(status.result().get("sum").asDouble()).isEqualTo(100.0);
            assertThat(status.result().get("count").asInt()).isEqualTo(4);
            assertThat(status.submittedAt()).isNotNull();
            assertThat(status.startedAt()).isNotNull().isAfterOrEqualTo(status.submittedAt());
            assertThat(status.finishedAt()).isNotNull().isAfterOrEqualTo(status.startedAt());
            assertThat(status.nextRetryAt()).isNull();
            assertThat(status.attempts()).hasSize(1);
            assertThat(status.attempts().get(0).status()).isEqualTo(AttemptStatus.SUCCEEDED);
        });
    }

    @Test
    void returns404ForUnknownIdsAnd400ForInvalidRequests() {
        ResponseEntity<String> unknownJob = restTemplate.getForEntity(
                "/api/v1/jobs/" + UUID.randomUUID(), String.class);
        assertThat(unknownJob.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        ResponseEntity<String> unknownDlq = restTemplate.getForEntity(
                "/api/v1/dlq/" + UUID.randomUUID(), String.class);
        assertThat(unknownDlq.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // Blank jobType -> 400
        ResponseEntity<String> blankType = postRaw("{\"jobType\":\"  \",\"payload\":{\"x\":1}}");
        assertThat(blankType.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        // Non-object payload -> 400
        ResponseEntity<String> arrayPayload = postRaw("{\"jobType\":\"echo\",\"payload\":[1,2,3]}");
        assertThat(arrayPayload.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        // Negative maxRetries -> 400
        ResponseEntity<String> negRetries = postRaw("{\"jobType\":\"echo\",\"maxRetries\":-1,\"payload\":{\"message\":\"hi\"}}");
        assertThat(negRetries.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        // Both scheduledAt and positive delayMs -> 400
        ResponseEntity<String> conflictingSchedule = postRaw(
                "{\"jobType\":\"echo\",\"scheduledAt\":\"2030-01-01T00:00:00Z\",\"delayMs\":1000,\"payload\":{\"message\":\"hi\"}}");
        assertThat(conflictingSchedule.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void exercisesScheduledQueuedRunningAndNonDlqFailedStatesExplicitly() {
        // 1. QUEUED (scheduled via delayMs) -> stays QUEUED before due time -> SUCCEEDED once due
        Instant beforeSubmit = Instant.now();
        ResponseEntity<JobAcceptedResponse> delayedResp = postJob(Map.of(
                "jobType", "echo",
                "delayMs", 700,
                "payload", Map.of("message", "deferred-hello")));
        String delayedId = delayedResp.getBody().jobId();

        JobStatusResponse initialQueued = getJob(delayedId);
        assertThat(initialQueued.state()).isEqualTo(JobStatus.QUEUED);
        assertThat(initialQueued.attemptCount()).isZero();
        assertThat(initialQueued.result()).isNull();
        assertThat(initialQueued.lastError()).isNull();
        assertThat(initialQueued.startedAt()).isNull();
        assertThat(initialQueued.finishedAt()).isNull();
        assertThat(initialQueued.nextRetryAt()).isNull();

        // Verify raw HTTP JSON explicitly contains null fields when unset
        ResponseEntity<String> rawQueuedJson = restTemplate.getForEntity("/api/v1/jobs/" + delayedId, String.class);
        assertThat(rawQueuedJson.getBody()).contains("\"state\":\"QUEUED\"")
                .contains("\"result\":null")
                .contains("\"lastError\":null")
                .contains("\"nextRetryAt\":null");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse done = getJob(delayedId);
            assertThat(done.state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(done.startedAt()).isAfterOrEqualTo(beforeSubmit.plusMillis(600));
            assertThat(done.result().get("echo").asText()).isEqualTo("deferred-hello");
        });

        // Also verify explicit scheduledAt timestamp
        Instant explicitSchedule = Instant.now().plusMillis(450);
        ResponseEntity<JobAcceptedResponse> atResp = postJob(Map.of(
                "jobType", "echo",
                "scheduledAt", explicitSchedule.toString(),
                "payload", Map.of("message", "scheduled-at")));
        String atId = atResp.getBody().jobId();
        assertThat(getJob(atId).state()).isEqualTo(JobStatus.QUEUED);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(getJob(atId).state()).isEqualTo(JobStatus.SUCCEEDED));

        // 2. RUNNING state observed mid-execution
        ResponseEntity<JobAcceptedResponse> runningResp = postJob(Map.of(
                "jobType", "simulate",
                "timeoutMs", 5000,
                "payload", Map.of("simulateDelayMs", 700, "resultValue", Map.of("done", true))));
        String runningJobId = runningResp.getBody().jobId();

        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(30)).untilAsserted(() -> {
            JobStatusResponse inFlight = getJob(runningJobId);
            assertThat(inFlight.state()).isEqualTo(JobStatus.RUNNING);
            assertThat(inFlight.attemptCount()).isEqualTo(1);
            assertThat(inFlight.startedAt()).isNotNull();
            assertThat(inFlight.finishedAt()).isNull();
            assertThat(inFlight.result()).isNull();
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse finished = getJob(runningJobId);
            assertThat(finished.state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(finished.result().path("resultValue").path("done").asBoolean()).isTrue();
        });

        // 3. FAILED state (non-DLQ terminal domain failure)
        ResponseEntity<JobAcceptedResponse> terminalResp = postJob(Map.of(
                "jobType", "simulate",
                "maxRetries", 4,
                "payload", Map.of("simulateTerminalFailure", true)));
        String failedJobId = terminalResp.getBody().jobId();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse failed = getJob(failedJobId);
            assertThat(failed.state()).isEqualTo(JobStatus.FAILED);
            assertThat(failed.attemptCount()).isEqualTo(1);
            assertThat(failed.result()).isNull();
            assertThat(failed.finishedAt()).isNotNull();
            assertThat(failed.nextRetryAt()).isNull();
            assertThat(failed.lastError()).isNotNull();
            assertThat(failed.lastError().type()).contains("TerminalJobFailureException");
            assertThat(failed.lastError().message()).contains("Simulated terminal domain failure");
        });

        // Verify non-DLQ FAILED job was NOT routed to DLQ
        ResponseEntity<String> notInDlq = restTemplate.getForEntity("/api/v1/dlq/" + failedJobId, String.class);
        assertThat(notInDlq.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void retriesWithNonBlockingJitteredBackoffRecordsLastErrorAndEventuallySucceeds() {
        ResponseEntity<JobAcceptedResponse> resp = postJob(Map.of(
                "jobType", "echo",
                "maxRetries", 3,
                "payload", Map.of(
                        "message", "resilient-job",
                        "simulateFailures", 2)));
        String jobId = resp.getBody().jobId();

        // Observe RUNNING -> QUEUED while awaiting retry (with nextRetryAt & lastError populated)
        await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> {
            JobStatusResponse mid = getJob(jobId);
            assertThat(mid.attemptCount()).isGreaterThanOrEqualTo(1);
            assertThat(mid.lastError()).isNotNull();
            assertThat(mid.lastError().type()).contains("SimulatedTransientFailureException");
            if (mid.state() == JobStatus.QUEUED) {
                assertThat(mid.nextRetryAt()).isNotNull();
                assertThat(mid.result()).isNull();
            }
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse finalState = getJob(jobId);
            assertThat(finalState.state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(finalState.attemptCount()).isEqualTo(3);
            assertThat(finalState.nextRetryAt()).isNull();
            assertThat(finalState.finishedAt()).isNotNull();
            assertThat(finalState.result()).isNotNull();
            assertThat(finalState.result().get("echo").asText()).isEqualTo("resilient-job");
            assertThat(finalState.result().get("attempt").asInt()).isEqualTo(3);
            assertThat(finalState.lastError()).isNotNull();
            assertThat(finalState.lastError().type()).contains("SimulatedTransientFailureException");
            assertThat(finalState.attempts()).hasSize(3);
            assertThat(finalState.attempts().get(0).status()).isEqualTo(AttemptStatus.FAILED);
            assertThat(finalState.attempts().get(0).retryable()).isTrue();
            assertThat(finalState.attempts().get(1).status()).isEqualTo(AttemptStatus.FAILED);
            assertThat(finalState.attempts().get(2).status()).isEqualTo(AttemptStatus.SUCCEEDED);
        });
    }

    @Test
    void enforcesPerAttemptTimeoutInterruptsWorkerRetriesUpToMaxRetriesAndRoutesToDlq() {
        long startNano = System.nanoTime();
        ResponseEntity<JobAcceptedResponse> resp = postJob(Map.of(
                "jobType", "simulate",
                "timeoutMs", 120,
                "maxRetries", 1,
                "payload", Map.of("simulateDelayMs", 10_000)));
        String jobId = resp.getBody().jobId();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse status = getJob(jobId);
            assertThat(status.state()).isEqualTo(JobStatus.DEAD_LETTERED);
            // 1 initial attempt + 1 retry = 2 total attempts
            assertThat(status.attemptCount()).isEqualTo(2);
            assertThat(status.result()).isNull();
            assertThat(status.finishedAt()).isNotNull();
            assertThat(status.nextRetryAt()).isNull();
            assertThat(status.lastError()).isNotNull();
            assertThat(status.lastError().type()).contains("JobAttemptTimeoutException");
            assertThat(status.attempts()).hasSize(2);
            assertThat(status.attempts()).allSatisfy(a ->
                    assertThat(a.status()).isEqualTo(AttemptStatus.TIMED_OUT));
        });

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        // Proves both attempts were interrupted around 120ms rather than waiting 10,000ms!
        assertThat(elapsedMs).isLessThan(5_000L);

        // Verify DeadLetterConsumer persisted diagnostic DLQ record
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ResponseEntity<DeadLetterJobResponse> dlqResp =
                    restTemplate.getForEntity("/api/v1/dlq/" + jobId, DeadLetterJobResponse.class);
            assertThat(dlqResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            DeadLetterJobResponse dlq = dlqResp.getBody();
            assertThat(dlq).isNotNull();
            assertThat(dlq.jobId()).isEqualTo(jobId);
            assertThat(dlq.failureReason()).isEqualTo("MAX_RETRIES_EXHAUSTED");
            assertThat(dlq.attemptCount()).isEqualTo(2);
            assertThat(dlq.errorType()).contains("JobAttemptTimeoutException");
            assertThat(dlq.stackTrace()).contains("JobAttemptTimeoutException");
        });

        // Verify diagnostic headers directly on the Kafka jobs.dlq topic record
        Map<String, Object> consumerProps = kafkaProperties.buildConsumerProperties();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-header-inspector-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer =
                new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer()) {
            consumer.subscribe(List.of("jobs.dlq"));
            ConsumerRecord<String, String> matched = null;
            long deadline = System.currentTimeMillis() + 5_000L;
            while (System.currentTimeMillis() < deadline && matched == null) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> rec : polled) {
                    if (jobId.equals(rec.key())) {
                        matched = rec;
                        break;
                    }
                }
            }
            assertThat(matched).isNotNull();
            assertThat(headerValue(matched, "x-job-id")).isEqualTo(jobId);
            assertThat(headerValue(matched, "x-job-priority")).isEqualTo("NORMAL");
            assertThat(headerValue(matched, "x-attempt-count")).isEqualTo("2");
            assertThat(headerValue(matched, "x-failure-reason")).isEqualTo("MAX_RETRIES_EXHAUSTED");
            assertThat(headerValue(matched, "x-error-type")).contains("JobAttemptTimeoutException");
        }
    }

    @Test
    void nonRetryableFailureBypassesRetriesRoutesImmediatelyToDlqAndSupportsReplay() {
        // Non-retryable payload error with maxRetries=5 must fail on attempt 1 with zero retries
        ResponseEntity<JobAcceptedResponse> badJobResp = postJob(Map.of(
                "jobType", "sum",
                "priority", "HIGH",
                "maxRetries", 5,
                "payload", Map.of("numbers", List.of("not-a-number"))));
        String badJobId = badJobResp.getBody().jobId();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse status = getJob(badJobId);
            assertThat(status.state()).isEqualTo(JobStatus.DEAD_LETTERED);
            assertThat(status.attemptCount()).isEqualTo(1);
            assertThat(status.result()).isNull();
            assertThat(status.lastError()).isNotNull();
            assertThat(status.lastError().type()).contains("InvalidPayloadException");
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ResponseEntity<DeadLetterJobResponse> dlqResp =
                    restTemplate.getForEntity("/api/v1/dlq/" + badJobId, DeadLetterJobResponse.class);
            assertThat(dlqResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(dlqResp.getBody().failureReason()).isEqualTo("NON_RETRYABLE");
            assertThat(dlqResp.getBody().originalTopic()).isEqualTo("jobs.high");
        });

        // Submit a job with maxRetries=0 and simulateFailures=1 -> hits DEAD_LETTERED on attempt 1,
        // then replay it via POST /api/v1/dlq/{jobId}/replay after fixing/replaying!
        // With simulateAlwaysFail=true and maxRetries=0, replay re-runs a fresh generation.
        ResponseEntity<JobAcceptedResponse> replayableResp = postJob(Map.of(
                "jobType", "echo",
                "maxRetries", 0,
                "payload", Map.of("message", "replay-me", "simulateAlwaysFail", true)));
        String replayJobId = replayableResp.getBody().jobId();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(getJob(replayJobId).state()).isEqualTo(JobStatus.DEAD_LETTERED);
            assertThat(restTemplate.getForEntity("/api/v1/dlq/" + replayJobId, DeadLetterJobResponse.class)
                    .getStatusCode()).isEqualTo(HttpStatus.OK);
        });

        // Update stored payload to remove simulateAlwaysFail (simulating data/config remediation) and replay
        JobEntity entity = jobRepository.findById(replayJobId).orElseThrow();
        entity.setPayloadJson("{\"message\":\"replayed-and-fixed\"}");
        jobRepository.save(entity);

        ResponseEntity<JobAcceptedResponse> replayed = restTemplate.postForEntity(
                "/api/v1/dlq/" + replayJobId + "/replay", null, JobAcceptedResponse.class);
        assertThat(replayed.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse afterReplay = getJob(replayJobId);
            assertThat(afterReplay.state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(afterReplay.replayCount()).isEqualTo(1);
            assertThat(afterReplay.attemptCount()).isEqualTo(1);
            assertThat(afterReplay.result().get("echo").asText()).isEqualTo("replayed-and-fixed");
        });

        ResponseEntity<List<DeadLetterJobResponse>> allDlq = restTemplate.exchange(
                "/api/v1/dlq",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});
        assertThat(allDlq.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(allDlq.getBody()).extracting(DeadLetterJobResponse::jobId)
                .contains(badJobId, replayJobId);
    }

    @Test
    void routesPrioritiesToDedicatedKafkaTopicsAndProcessesHighAheadOfLowBacklog() {
        CountingHandlerConfig.COMPLETION_ORDER.clear();

        // Enqueue a backlog of slow LOW-priority jobs (single low consumer thread)
        List<String> lowIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ResponseEntity<JobAcceptedResponse> low = postJob(Map.of(
                    "jobType", "counting.work",
                    "priority", "LOW",
                    "payload", Map.of("tag", "low-" + i, "workMs", 180)));
            lowIds.add(low.getBody().jobId());
        }

        // Submit HIGH and NORMAL jobs while LOW backlog is busy
        ResponseEntity<JobAcceptedResponse> high = postJob(Map.of(
                "jobType", "counting.work",
                "priority", "HIGH",
                "payload", Map.of("tag", "high-urgent", "workMs", 10)));
        ResponseEntity<JobAcceptedResponse> normal = postJob(Map.of(
                "jobType", "counting.work",
                "priority", "NORMAL",
                "payload", Map.of("tag", "normal-standard", "workMs", 10)));

        String highId = high.getBody().jobId();
        String normalId = normal.getBody().jobId();

        // Verify outbox topic routing per priority
        assertThat(outboxEventRepository.findByJobIdOrderByCreatedAtAsc(highId))
                .extracting(OutboxEventEntity::getTopic)
                .containsExactly("jobs.high");
        assertThat(outboxEventRepository.findByJobIdOrderByCreatedAtAsc(normalId))
                .extracting(OutboxEventEntity::getTopic)
                .containsExactly("jobs.normal");
        assertThat(outboxEventRepository.findByJobIdOrderByCreatedAtAsc(lowIds.get(0)))
                .extracting(OutboxEventEntity::getTopic)
                .containsExactly("jobs.low");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(getJob(highId).state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(getJob(normalId).state()).isEqualTo(JobStatus.SUCCEEDED);
            for (String lowId : lowIds) {
                assertThat(getJob(lowId).state()).isEqualTo(JobStatus.SUCCEEDED);
            }
        });

        int highCompletionIdx = CountingHandlerConfig.COMPLETION_ORDER.indexOf("HIGH:high-urgent");
        int lastLowCompletionIdx = CountingHandlerConfig.COMPLETION_ORDER.lastIndexOf("LOW:low-4");
        assertThat(highCompletionIdx).isGreaterThanOrEqualTo(0).isLessThan(lastLowCompletionIdx);
    }

    @Test
    void guaranteesIdempotentSubmissionDuplicateKafkaDeliverySafetyAndLeaseFencingRecovery() throws Exception {
        // 1. 10 concurrent submissions with identical idempotencyKey -> single job created & executed once
        String sharedKey = "idem-" + UUID.randomUUID();
        String tag = "idem-tag-" + UUID.randomUUID();
        int callers = 10;
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch ready = new CountDownLatch(callers);
        CountDownLatch start = new CountDownLatch(1);

        List<Callable<JobAcceptedResponse>> tasks = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            tasks.add(() -> {
                ready.countDown();
                start.await(5, TimeUnit.SECONDS);
                ResponseEntity<JobAcceptedResponse> r = postJob(Map.of(
                        "jobType", "counting.work",
                        "idempotencyKey", sharedKey,
                        "payload", Map.of("tag", tag)));
                assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
                return r.getBody();
            });
        }

        List<Future<JobAcceptedResponse>> futures = new ArrayList<>();
        for (Callable<JobAcceptedResponse> task : tasks) {
            futures.add(pool.submit(task));
        }
        ready.await(5, TimeUnit.SECONDS);
        start.countDown();

        Set<String> returnedJobIds = ConcurrentHashMap.newKeySet();
        int deduplicatedResponses = 0;
        for (Future<JobAcceptedResponse> f : futures) {
            JobAcceptedResponse body = f.get(10, TimeUnit.SECONDS);
            returnedJobIds.add(body.jobId());
            if (body.deduplicated()) {
                deduplicatedResponses++;
            }
        }
        pool.shutdownNow();

        assertThat(returnedJobIds).hasSize(1);
        assertThat(deduplicatedResponses).isEqualTo(callers - 1);
        String uniqueJobId = returnedJobIds.iterator().next();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(getJob(uniqueJobId).state()).isEqualTo(JobStatus.SUCCEEDED));

        // Inject duplicate Kafka dispatches for the already-completed job -> must be ignored idempotently
        JobDispatchEnvelope duplicateEnvelope = new JobDispatchEnvelope(
                uniqueJobId, "counting.work", JobPriority.NORMAL, 1, 0, Instant.now());
        String payloadJson = objectMapper.writeValueAsString(duplicateEnvelope);
        for (int i = 0; i < 3; i++) {
            kafkaTemplate.send("jobs.normal", uniqueJobId, payloadJson).get(5, TimeUnit.SECONDS);
        }
        Thread.sleep(250);
        assertThat(CountingHandlerConfig.INVOCATIONS.get(tag).get()).isEqualTo(1);
        assertThat(getJob(uniqueJobId).attemptCount()).isEqualTo(1);

        // 2. Stale lease expiration recovery + zombie fencing check
        ResponseEntity<JobAcceptedResponse> leaseJobResp = postJob(Map.of(
                "jobType", "echo",
                "delayMs", 60_000, // hold initial outbox so we can claim manually and simulate worker crash
                "maxRetries", 2,
                "payload", Map.of("message", "recovered-from-crash")));
        String crashedJobId = leaseJobResp.getBody().jobId();

        JobDispatchEnvelope manualEnvelope = new JobDispatchEnvelope(
                crashedJobId, "echo", JobPriority.NORMAL, 1, 0, Instant.now());
        ClaimedJobAttempt claimed = workerStateTxService.tryClaimAttempt(manualEnvelope).orElseThrow();
        assertThat(getJob(crashedJobId).state()).isEqualTo(JobStatus.RUNNING);

        // Force lease expiration to simulate crashed worker node
        JobEntity runningEntity = jobRepository.findById(crashedJobId).orElseThrow();
        runningEntity.setLeaseExpiresAt(Instant.now().minusSeconds(5));
        jobRepository.saveAndFlush(runningEntity);

        int recovered = workerStateTxService.recoverExpiredRunningJobs(Instant.now(), 10);
        assertThat(recovered).isGreaterThanOrEqualTo(1);

        // Zombie worker wakes up with stale leaseToken -> completion MUST be fenced (rejected)!
        boolean zombieAccepted = workerStateTxService.completeSuccess(
                claimed, objectMapper.createObjectNode().put("echo", "zombie-write"));
        assertThat(zombieAccepted).isFalse();

        // Meanwhile, the recovered retry executes cleanly to SUCCEEDED on attempt 2!
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            JobStatusResponse status = getJob(crashedJobId);
            assertThat(status.state()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(status.attemptCount()).isEqualTo(2);
            assertThat(status.result().get("echo").asText()).isEqualTo("recovered-from-crash");
        });

        // 3. Concurrent worker CAS claiming race: 8 threads racing on the exact same QUEUED job -> exactly 1 winner
        ResponseEntity<JobAcceptedResponse> raceJobResp = postJob(Map.of(
                "jobType", "echo",
                "delayMs", 60_000,
                "payload", Map.of("message", "cas-race")));
        String raceJobId = raceJobResp.getBody().jobId();
        JobDispatchEnvelope raceEnvelope = new JobDispatchEnvelope(
                raceJobId, "echo", JobPriority.NORMAL, 1, 0, Instant.now());

        int racers = 8;
        ExecutorService claimPool = Executors.newFixedThreadPool(racers);
        CountDownLatch claimReady = new CountDownLatch(racers);
        CountDownLatch claimGo = new CountDownLatch(1);
        List<Future<Boolean>> claimFutures = new ArrayList<>();
        for (int i = 0; i < racers; i++) {
            claimFutures.add(claimPool.submit(() -> {
                claimReady.countDown();
                claimGo.await(5, TimeUnit.SECONDS);
                return workerStateTxService.tryClaimAttempt(raceEnvelope).isPresent();
            }));
        }
        claimReady.await(5, TimeUnit.SECONDS);
        claimGo.countDown();

        int winners = 0;
        for (Future<Boolean> f : claimFutures) {
            if (f.get(5, TimeUnit.SECONDS)) {
                winners++;
            }
        }
        claimPool.shutdownNow();
        assertThat(winners).isEqualTo(1);

        // 4. Concurrent Outbox FOR UPDATE SKIP LOCKED claiming race -> disjoint claimed event IDs
        List<String> dueOutboxJobIds = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ResponseEntity<JobAcceptedResponse> r = postJob(Map.of(
                    "jobType", "echo",
                    "delayMs", 120_000,
                    "payload", Map.of("message", "outbox-skip-locked-" + i)));
            dueOutboxJobIds.add(r.getBody().jobId());
        }
        // Make those 6 outbox events due at a future timestamp we pass into claimDueBatch
        Instant futureCheck = Instant.now().plusSeconds(180);
        int pollers = 4;
        ExecutorService outboxPool = Executors.newFixedThreadPool(pollers);
        CountDownLatch outboxReady = new CountDownLatch(pollers);
        CountDownLatch outboxGo = new CountDownLatch(1);
        List<Future<List<OutboxEventEntity>>> outboxFutures = new ArrayList<>();
        for (int i = 0; i < pollers; i++) {
            outboxFutures.add(outboxPool.submit(() -> {
                outboxReady.countDown();
                outboxGo.await(5, TimeUnit.SECONDS);
                List<OutboxEventEntity> claimedByWorker = new ArrayList<>();
                while (true) {
                    List<OutboxEventEntity> batch = outboxTxService.claimDueBatch(futureCheck, 2);
                    if (batch.isEmpty()) {
                        break;
                    }
                    claimedByWorker.addAll(batch);
                }
                return claimedByWorker;
            }));
        }
        outboxReady.await(5, TimeUnit.SECONDS);
        outboxGo.countDown();

        List<String> allClaimedEventIds = new ArrayList<>();
        for (Future<List<OutboxEventEntity>> f : outboxFutures) {
            for (OutboxEventEntity ev : f.get(5, TimeUnit.SECONDS)) {
                allClaimedEventIds.add(ev.getId());
            }
        }
        outboxPool.shutdownNow();
        // Every claimed outbox event ID across all concurrent pollers must be strictly unique (zero double-claim)
        assertThat(allClaimedEventIds).doesNotHaveDuplicates();
        assertThat(allClaimedEventIds.size()).isGreaterThanOrEqualTo(6);
    }

    private String headerValue(ConsumerRecord<String, String> record, String headerName) {
        var header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value(), java.nio.charset.StandardCharsets.UTF_8) : null;
    }

    private ResponseEntity<JobAcceptedResponse> postJob(Map<String, Object> requestMap) {
        return restTemplate.postForEntity("/api/v1/jobs", requestMap, JobAcceptedResponse.class);
    }

    private ResponseEntity<String> postRaw(String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return restTemplate.postForEntity("/api/v1/jobs", new HttpEntity<>(json, headers), String.class);
    }

    private JobStatusResponse getJob(String jobId) {
        ResponseEntity<JobStatusResponse> response =
                restTemplate.getForEntity("/api/v1/jobs/" + jobId, JobStatusResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        return response.getBody();
    }
}
