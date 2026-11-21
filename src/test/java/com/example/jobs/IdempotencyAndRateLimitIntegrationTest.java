package com.example.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.OutboxEventRepository;
import com.example.jobs.web.dto.ApiErrorResponse;
import com.example.jobs.web.dto.JobAcceptedResponse;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.jobs.rate-limit.limit=10",
                "app.jobs.rate-limit.window-ms=3000",
                "app.jobs.idempotency.await-ms=300",
                "app.jobs.idempotency.fingerprint-window-ms=2000"
        })
@EmbeddedKafka(partitions = 3, topics = {"jobs.high", "jobs.normal", "jobs.low", "jobs.dlq"})
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class IdempotencyAndRateLimitIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void duplicateIsAnsweredFromRedisWithoutReadingTheDatabase() {
        String client = newClient();
        String key = "idem-" + UUID.randomUUID();
        JobAcceptedResponse first = submit(client, key).getBody();
        assertThat(first.deduplicated()).isFalse();
        assertThat(redis.opsForValue().get("jobs:idem:" + key)).contains(first.jobId());

        // Remove the job from the database: only a Redis-served duplicate can still return it.
        outboxEventRepository.deleteAll(outboxEventRepository.findAll().stream()
                .filter(e -> e.getJobId().equals(first.jobId())).toList());
        jobRepository.deleteById(first.jobId());

        ResponseEntity<JobAcceptedResponse> duplicate = submit(client, key);
        assertThat(duplicate.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(duplicate.getBody().jobId()).isEqualTo(first.jobId());
        assertThat(duplicate.getBody().deduplicated()).isTrue();
    }

    @Test
    void keyLostFromRedisIsStillDeduplicatedByTheDatabaseConstraint() {
        String client = newClient();
        String key = "idem-" + UUID.randomUUID();
        JobAcceptedResponse first = submit(client, key).getBody();

        redis.delete("jobs:idem:" + key);

        JobAcceptedResponse duplicate = submit(client, key).getBody();
        assertThat(duplicate.jobId()).isEqualTo(first.jobId());
        assertThat(duplicate.deduplicated()).isTrue();
        assertThat(redis.opsForValue().get("jobs:idem:" + key)).contains(first.jobId());
        assertThat(jobRepository.findAll().stream().filter(j -> key.equals(j.getIdempotencyKey()))).hasSize(1);
    }

    @Test
    void requestStillInFlightOnAnotherNodeGets409UntilItFinishes() {
        String client = newClient();
        String key = "idem-" + UUID.randomUUID();
        redis.opsForValue().set("jobs:idem:" + key, "PENDING:other-node");

        ResponseEntity<ApiErrorResponse> conflict = restTemplate.exchange("/api/v1/jobs", HttpMethod.POST,
                new HttpEntity<>(job(key), headers(client)), ApiErrorResponse.class);
        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(conflict.getBody().message()).contains("still being processed");

        redis.delete("jobs:idem:" + key);
        assertThat(submit(client, key).getBody().deduplicated()).isFalse();
    }

    @Test
    void withoutAKeyIdenticalRequestsFromOneClientCollapseWithinTheFingerprintWindow() throws Exception {
        String client = newClient();
        String tag = UUID.randomUUID().toString();
        JobAcceptedResponse first = submitRaw(client, null,
                "{\"jobType\":\"echo\",\"delayMs\":60000,\"payload\":{\"message\":\"m\",\"tag\":\"" + tag + "\"}}");
        assertThat(first.deduplicated()).isFalse();
        assertThat(first.idempotencyKey()).startsWith("fp:");

        // Same request with payload keys reordered: same fingerprint.
        JobAcceptedResponse retry = submitRaw(client, null,
                "{\"payload\":{\"tag\":\"" + tag + "\",\"message\":\"m\"},\"delayMs\":60000,\"jobType\":\"echo\"}");
        assertThat(retry.jobId()).isEqualTo(first.jobId());
        assertThat(retry.deduplicated()).isTrue();

        String body = "{\"jobType\":\"echo\",\"delayMs\":60000,\"payload\":{\"message\":\"m\",\"tag\":\"" + tag + "\"}}";
        assertThat(submitRaw(newClient(), null, body).jobId()).isNotEqualTo(first.jobId());
        assertThat(submitRaw(client, null, body.replace("\"m\"", "\"other\"")).jobId()).isNotEqualTo(first.jobId());

        Thread.sleep(2_100);
        JobAcceptedResponse afterWindow = submitRaw(client, null, body);
        assertThat(afterWindow.deduplicated()).isFalse();
        assertThat(afterWindow.jobId()).isNotEqualTo(first.jobId());
    }

    @Test
    void eventIdDeduplicatesPerClientAndFallsBackToTheDatabaseWhenRedisLosesTheKey() {
        String client = newClient();
        String eventId = "evt-" + UUID.randomUUID();
        JobAcceptedResponse first = submitRaw(client, eventId,
                "{\"jobType\":\"echo\",\"delayMs\":60000,\"payload\":{\"message\":\"v1\"}}");
        assertThat(first.idempotencyKey()).startsWith("evt:");

        // The event id is the identity: a redelivered event is the same job even if the body differs.
        JobAcceptedResponse redelivered = submitRaw(client, eventId,
                "{\"jobType\":\"echo\",\"delayMs\":60000,\"payload\":{\"message\":\"v2\"}}");
        assertThat(redelivered.jobId()).isEqualTo(first.jobId());
        assertThat(redelivered.deduplicated()).isTrue();

        assertThat(submitRaw(newClient(), eventId,
                "{\"jobType\":\"echo\",\"delayMs\":60000,\"payload\":{\"message\":\"v1\"}}").jobId())
                .isNotEqualTo(first.jobId());

        double dbDedupBefore = meterRegistry.counter("jobs.submissions.deduplicated",
                "source", "database", "key", "event").count();
        redis.delete("jobs:idem:" + first.idempotencyKey());
        JobAcceptedResponse afterRedisLoss = submitRaw(client, eventId,
                "{\"jobType\":\"echo\",\"delayMs\":60000,\"payload\":{\"message\":\"v1\"}}");
        assertThat(afterRedisLoss.jobId()).isEqualTo(first.jobId());
        assertThat(afterRedisLoss.deduplicated()).isTrue();
        assertThat(meterRegistry.counter("jobs.submissions.deduplicated", "source", "database", "key", "event").count())
                .isEqualTo(dbDedupBefore + 1);
    }

    @Test
    void eachClientGetsTenRequestsPerThreeSecondsAcrossConcurrentCalls() throws Exception {
        String client = newClient();
        int callers = 30;
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseEntity<String>>> results = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return restTemplate.exchange("/api/v1/jobs", HttpMethod.POST,
                        new HttpEntity<>(job(null), headers(client)), String.class);
            }));
        }
        start.countDown();
        int accepted = 0;
        int limited = 0;
        for (Future<ResponseEntity<String>> f : results) {
            ResponseEntity<String> r = f.get();
            if (r.getStatusCode() == HttpStatus.ACCEPTED) {
                accepted++;
            } else if (r.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                limited++;
                assertThat(Integer.parseInt(r.getHeaders().getFirst("Retry-After"))).isBetween(1, 3);
                assertThat(r.getHeaders().getFirst("X-RateLimit-Remaining")).isEqualTo("0");
            }
        }
        pool.shutdown();
        assertThat(accepted).isEqualTo(10);
        assertThat(limited).isEqualTo(20);

        assertThat(submit(newClient(), null).getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(restTemplate.exchange("/api/v1/ops/queue", HttpMethod.GET, new HttpEntity<>(headers(client)),
                String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(restTemplate.exchange("/api/v1/jobs/unknown", HttpMethod.GET, new HttpEntity<>(headers(client)),
                String.class).getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        Thread.sleep(3_100);
        ResponseEntity<JobAcceptedResponse> afterWindow = submit(client, null);
        assertThat(afterWindow.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(afterWindow.getHeaders().getFirst("X-RateLimit-Remaining")).isEqualTo("9");
    }

    private ResponseEntity<JobAcceptedResponse> submit(String client, String key) {
        return restTemplate.exchange("/api/v1/jobs", HttpMethod.POST, new HttpEntity<>(job(key), headers(client)),
                JobAcceptedResponse.class);
    }

    private JobAcceptedResponse submitRaw(String client, String eventId, String json) {
        HttpHeaders headers = headers(client);
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (eventId != null) {
            headers.set("X-Event-Id", eventId);
        }
        ResponseEntity<JobAcceptedResponse> response = restTemplate.exchange("/api/v1/jobs", HttpMethod.POST,
                new HttpEntity<>(json, headers), JobAcceptedResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return response.getBody();
    }

    private Map<String, Object> job(String key) {
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("jobType", "echo");
        body.put("delayMs", 60_000);
        body.put("payload", Map.of("message", "hi"));
        if (key != null) {
            body.put("idempotencyKey", key);
        }
        return body;
    }

    private HttpHeaders headers(String client) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Client-Id", client);
        return headers;
    }

    private String newClient() {
        return "client-" + UUID.randomUUID();
    }
}
