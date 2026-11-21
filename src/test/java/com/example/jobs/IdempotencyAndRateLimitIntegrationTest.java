package com.example.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.OutboxEventRepository;
import com.example.jobs.web.dto.ApiErrorResponse;
import com.example.jobs.web.dto.JobAcceptedResponse;
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
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.jobs.rate-limit.limit=10",
                "app.jobs.rate-limit.window-ms=3000",
                "app.jobs.idempotency.await-ms=300"
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
