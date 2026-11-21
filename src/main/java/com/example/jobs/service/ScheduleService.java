package com.example.jobs.service;

import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobScheduleEntity;
import com.example.jobs.exception.JobStateConflictException;
import com.example.jobs.exception.RequestValidationException;
import com.example.jobs.exception.ScheduleNotFoundException;
import com.example.jobs.repository.JobScheduleRepository;
import com.example.jobs.web.dto.CreateJobRequest;
import com.example.jobs.web.dto.CreateScheduleRequest;
import com.example.jobs.web.dto.ScheduleResponse;
import com.example.jobs.worker.JobHandlerRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Cron-style recurring jobs. Each due fire creates an ordinary job (same outbox, retry and DLQ path as
 * {@code POST /api/v1/jobs}) and advances {@code next_run_at} in the same transaction, under a
 * {@code FOR UPDATE SKIP LOCKED} row lock, so each fire time produces exactly one job across all replicas.
 */
@Service
public class ScheduleService {

    private final JobScheduleRepository scheduleRepository;
    private final JobWriteTxService jobWriteTxService;
    private final JobHandlerRegistry jobHandlerRegistry;
    private final ObjectMapper objectMapper;

    public ScheduleService(
            JobScheduleRepository scheduleRepository,
            JobWriteTxService jobWriteTxService,
            JobHandlerRegistry jobHandlerRegistry,
            ObjectMapper objectMapper) {
        this.scheduleRepository = scheduleRepository;
        this.jobWriteTxService = jobWriteTxService;
        this.jobHandlerRegistry = jobHandlerRegistry;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ScheduleResponse create(CreateScheduleRequest request) {
        CreateJobRequest job = request.job();
        jobHandlerRegistry.requireSupported("job.jobType", job.jobType());
        if (job.payload() == null || !job.payload().isObject()) {
            throw new RequestValidationException("job.payload", "job.payload must be a JSON object");
        }
        if (job.jobId() != null || job.scheduledAt() != null || job.delayMs() != null || job.idempotencyKey() != null) {
            throw new IllegalArgumentException(
                    "jobId, scheduledAt, delayMs and idempotencyKey are not allowed in a schedule's job");
        }
        String name = request.name().trim();
        if (scheduleRepository.existsByName(name)) {
            throw new JobStateConflictException("Schedule name already exists: " + name);
        }

        String cron = normalizeCron(request.cron());
        String timeZone = request.timeZone() == null || request.timeZone().isBlank() ? "UTC" : request.timeZone().trim();
        Instant now = Instant.now();

        JobScheduleEntity schedule = new JobScheduleEntity();
        schedule.setId(UUID.randomUUID().toString());
        schedule.setName(name);
        schedule.setCron(cron);
        schedule.setTimeZone(timeZone);
        schedule.setJobTemplateJson(writeJson(job));
        schedule.setEnabled(true);
        schedule.setNextRunAt(nextFireAfter(cron, timeZone, now));
        if (schedule.getNextRunAt() == null) {
            throw new IllegalArgumentException("cron never fires: " + request.cron());
        }
        schedule.setCreatedAt(now);
        schedule.setUpdatedAt(now);
        try {
            return toResponse(scheduleRepository.saveAndFlush(schedule));
        } catch (DataIntegrityViolationException ex) {
            throw new JobStateConflictException("Schedule name already exists: " + name);
        }
    }

    @Transactional(readOnly = true)
    public List<ScheduleResponse> list() {
        return scheduleRepository.findAllByOrderByNameAsc().stream().map(this::toResponse).toList();
    }

    @Transactional(readOnly = true)
    public ScheduleResponse get(String id) {
        return toResponse(find(id));
    }

    @Transactional
    public ScheduleResponse setEnabled(String id, boolean enabled) {
        JobScheduleEntity schedule = find(id);
        Instant now = Instant.now();
        schedule.setEnabled(enabled);
        schedule.setNextRunAt(enabled ? nextFireAfter(schedule.getCron(), schedule.getTimeZone(), now) : null);
        schedule.setUpdatedAt(now);
        return toResponse(schedule);
    }

    @Transactional
    public void delete(String id) {
        scheduleRepository.delete(find(id));
    }

    /**
     * Fires every due schedule once. Fire times missed while no replica was running are coalesced into a
     * single run; the next fire time is then computed from {@code now}.
     */
    @Transactional
    public int fireDue(Instant now, int limit) {
        List<JobScheduleEntity> due = scheduleRepository.lockDue(now, limit);
        for (JobScheduleEntity schedule : due) {
            CreateJobRequest template = readTemplate(schedule.getJobTemplateJson());
            Map<String, String> metadata = new LinkedHashMap<>(template.metadata() != null ? template.metadata() : Map.of());
            metadata.put("scheduleId", schedule.getId());
            metadata.put("scheduledFireTime", schedule.getNextRunAt().toString());
            CreateJobRequest request = new CreateJobRequest(null, template.jobType(), template.payload(), null, null,
                    template.priority(), template.maxRetries(), template.timeoutMs(), null, metadata,
                    template.routeToDlq());

            JobEntity job = jobWriteTxService.insertJobAndOutbox(request, null, now, now);

            Instant next = nextFireAfter(schedule.getCron(), schedule.getTimeZone(), now);
            schedule.setLastRunAt(schedule.getNextRunAt());
            schedule.setLastJobId(job.getId());
            schedule.setNextRunAt(next);
            schedule.setEnabled(next != null);
            schedule.setUpdatedAt(now);
        }
        return due.size();
    }

    /** Accepts Spring 6-field cron (seconds first), classic 5-field Unix cron, and macros like {@code @hourly}. */
    static String normalizeCron(String cron) {
        String trimmed = cron.trim().replaceAll("\\s+", " ");
        String normalized = trimmed.split(" ").length == 5 ? "0 " + trimmed : trimmed;
        if (!CronExpression.isValidExpression(normalized)) {
            throw new IllegalArgumentException("Invalid cron expression: " + cron);
        }
        return normalized;
    }

    private static Instant nextFireAfter(String cron, String timeZone, Instant after) {
        ZoneId zone;
        try {
            zone = ZoneId.of(timeZone);
        } catch (DateTimeException ex) {
            throw new IllegalArgumentException("Invalid timeZone: " + timeZone);
        }
        ZonedDateTime next = CronExpression.parse(cron).next(after.atZone(zone));
        return next != null ? next.toInstant() : null;
    }

    private JobScheduleEntity find(String id) {
        return scheduleRepository.findById(id).orElseThrow(() -> new ScheduleNotFoundException(id));
    }

    private ScheduleResponse toResponse(JobScheduleEntity s) {
        return new ScheduleResponse(s.getId(), s.getName(), s.getCron(), s.getTimeZone(), s.isEnabled(),
                s.getNextRunAt(), s.getLastRunAt(), s.getLastJobId(), readTemplate(s.getJobTemplateJson()),
                s.getCreatedAt());
    }

    private CreateJobRequest readTemplate(String json) {
        try {
            return objectMapper.readValue(json, CreateJobRequest.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Corrupt schedule job template", e);
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JSON", e);
        }
    }
}
