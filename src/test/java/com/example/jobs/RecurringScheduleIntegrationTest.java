package com.example.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobScheduleEntity;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.JobScheduleRepository;
import com.example.jobs.service.ScheduleService;
import com.example.jobs.web.dto.JobStatusResponse;
import com.example.jobs.web.dto.ScheduleResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = {"jobs.high", "jobs.normal", "jobs.low", "jobs.dlq"})
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RecurringScheduleIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobScheduleRepository scheduleRepository;

    @Autowired
    private ScheduleService scheduleService;

    @Test
    void everySecondScheduleRunsJobsUntilPausedAndCanBeDeleted() throws Exception {
        ScheduleResponse schedule = create(schedule("* * * * * *", null,
                Map.of("jobType", "echo", "payload", Map.of("message", "tick"), "metadata", Map.of("team", "ops"))))
                .getBody();
        assertThat(schedule.enabled()).isTrue();
        assertThat(schedule.nextRunAt()).isAfter(schedule.createdAt());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<JobEntity> jobs = jobsOf(schedule.id());
            assertThat(jobs).hasSizeGreaterThanOrEqualTo(2);
            for (JobEntity job : jobs) {
                JobStatusResponse status = getJob(job.getId());
                assertThat(status.state()).isEqualTo(JobStatus.SUCCEEDED);
                assertThat(status.result().get("echo").asText()).isEqualTo("tick");
                assertThat(status.metadata()).containsEntry("team", "ops").containsEntry("scheduleId", schedule.id());
            }
        });
        ScheduleResponse fired = restTemplate.getForObject("/api/v1/schedules/" + schedule.id(), ScheduleResponse.class);
        assertThat(fired.lastJobId()).isNotNull();
        assertThat(fired.lastRunAt()).isNotNull();

        ScheduleResponse paused = restTemplate.postForObject(
                "/api/v1/schedules/" + schedule.id() + "/pause", null, ScheduleResponse.class);
        assertThat(paused.enabled()).isFalse();
        assertThat(paused.nextRunAt()).isNull();
        int jobsWhenPaused = jobsOf(schedule.id()).size();
        Thread.sleep(2_500);
        assertThat(jobsOf(schedule.id())).hasSize(jobsWhenPaused);

        ScheduleResponse resumed = restTemplate.postForObject(
                "/api/v1/schedules/" + schedule.id() + "/resume", null, ScheduleResponse.class);
        assertThat(resumed.enabled()).isTrue();
        await().atMost(Duration.ofSeconds(10)).until(() -> jobsOf(schedule.id()).size() > jobsWhenPaused);

        restTemplate.delete("/api/v1/schedules/" + schedule.id());
        assertThat(restTemplate.getForEntity("/api/v1/schedules/" + schedule.id(), String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void missedFiresCoalesceIntoExactlyOneJobAcrossConcurrentReplicas() throws Exception {
        ScheduleResponse schedule = create(schedule("0 0 0 1 1 *", null,
                Map.of("jobType", "echo", "payload", Map.of("message", "catch-up")))).getBody();

        JobScheduleEntity entity = scheduleRepository.findById(schedule.id()).orElseThrow();
        entity.setNextRunAt(Instant.now().minus(3, ChronoUnit.HOURS));
        scheduleRepository.saveAndFlush(entity);

        int replicas = 4;
        ExecutorService pool = Executors.newFixedThreadPool(replicas);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Integer>> fired = new ArrayList<>();
        for (int i = 0; i < replicas; i++) {
            fired.add(pool.submit(() -> {
                start.await();
                return scheduleService.fireDue(Instant.now(), 10);
            }));
        }
        start.countDown();
        for (Future<Integer> f : fired) {
            f.get();
        }
        pool.shutdown();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<JobEntity> jobs = jobsOf(schedule.id());
            assertThat(jobs).hasSize(1);
            assertThat(getJob(jobs.get(0).getId()).state()).isEqualTo(JobStatus.SUCCEEDED);
        });
        Thread.sleep(500);
        assertThat(jobsOf(schedule.id())).hasSize(1);

        ScheduleResponse after = restTemplate.getForObject("/api/v1/schedules/" + schedule.id(), ScheduleResponse.class);
        assertThat(after.nextRunAt()).isAfter(Instant.now());
        assertThat(after.nextRunAt().atZone(ZoneId.of("UTC")).getDayOfYear()).isEqualTo(1);
    }

    @Test
    void acceptsUnixCronAndTimeZonesAndRejectsInvalidSchedules() {
        ScheduleResponse unix = create(schedule("*/5 * * * *", null, echoJob())).getBody();
        assertThat(unix.cron()).isEqualTo("0 */5 * * * *");
        assertThat(unix.timeZone()).isEqualTo("UTC");
        ZonedDateTime next = unix.nextRunAt().atZone(ZoneId.of("UTC"));
        assertThat(next.getSecond()).isZero();
        assertThat(next.getMinute() % 5).isZero();

        ScheduleResponse ist = create(schedule("0 0 9 * * MON-FRI", "Asia/Kolkata", echoJob())).getBody();
        ZonedDateTime istNext = ist.nextRunAt().atZone(ZoneId.of("Asia/Kolkata"));
        assertThat(istNext.getHour()).isEqualTo(9);
        assertThat(istNext.getMinute()).isZero();
        assertThat(istNext.getDayOfWeek().getValue()).isBetween(1, 5);

        assertThat(create(schedule("not a cron", null, echoJob())).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(create(schedule("0 0 * * * *", "Mars/Olympus", echoJob())).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(create(schedule("0 0 * * * *", null,
                Map.of("jobType", "echo", "payload", Map.of(), "delayMs", 1000))).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(create(schedule("0 0 * * * *", null, Map.of("jobType", "echo"))).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        ResponseEntity<String> unknownType = restTemplate.postForEntity("/api/v1/schedules",
                schedule("0 0 * * * *", null, Map.of("jobType", "nope", "payload", Map.of())), String.class);
        assertThat(unknownType.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(unknownType.getBody()).contains("\"job.jobType\":\"Unsupported jobType 'nope'");

        Map<String, Object> duplicate = schedule("0 0 * * * *", null, echoJob());
        duplicate.put("name", unix.name());
        assertThat(create(duplicate).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        assertThat(restTemplate.exchange("/api/v1/schedules/missing/pause", HttpMethod.POST, null, String.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(restTemplate.getForObject("/api/v1/schedules", ScheduleResponse[].class))
                .extracting(ScheduleResponse::id).contains(unix.id(), ist.id());
    }

    private Map<String, Object> echoJob() {
        return Map.of("jobType", "echo", "payload", Map.of("message", "hi"));
    }

    private Map<String, Object> schedule(String cron, String timeZone, Map<String, Object> job) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", "schedule-" + UUID.randomUUID());
        body.put("cron", cron);
        if (timeZone != null) {
            body.put("timeZone", timeZone);
        }
        body.put("job", job);
        return body;
    }

    private ResponseEntity<ScheduleResponse> create(Map<String, Object> body) {
        return restTemplate.postForEntity("/api/v1/schedules", body, ScheduleResponse.class);
    }

    private List<JobEntity> jobsOf(String scheduleId) {
        return jobRepository.findAll().stream()
                .filter(j -> j.getMetadataJson() != null && j.getMetadataJson().contains(scheduleId))
                .toList();
    }

    private JobStatusResponse getJob(String jobId) {
        return restTemplate.getForObject("/api/v1/jobs/" + jobId, JobStatusResponse.class);
    }
}
