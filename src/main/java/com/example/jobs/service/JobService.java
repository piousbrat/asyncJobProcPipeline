package com.example.jobs.service;

import com.example.jobs.domain.DeadLetterRecordEntity;
import com.example.jobs.domain.JobAttemptEntity;
import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.exception.JobNotFoundException;
import com.example.jobs.outbox.OutboxRelayScheduler;
import com.example.jobs.repository.DeadLetterRecordRepository;
import com.example.jobs.repository.JobAttemptRepository;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.web.dto.CreateJobRequest;
import com.example.jobs.web.dto.DeadLetterJobResponse;
import com.example.jobs.web.dto.JobAcceptedResponse;
import com.example.jobs.web.dto.JobStatusResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobService {

    private static final TypeReference<Map<String, String>> METADATA_TYPE = new TypeReference<>() {};

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final DeadLetterRecordRepository deadLetterRecordRepository;
    private final JobWriteTxService jobWriteTxService;
    private final OutboxRelayScheduler outboxRelayScheduler;
    private final ObjectMapper objectMapper;

    public JobService(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository,
            DeadLetterRecordRepository deadLetterRecordRepository,
            JobWriteTxService jobWriteTxService,
            OutboxRelayScheduler outboxRelayScheduler,
            ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.deadLetterRecordRepository = deadLetterRecordRepository;
        this.jobWriteTxService = jobWriteTxService;
        this.outboxRelayScheduler = outboxRelayScheduler;
        this.objectMapper = objectMapper;
    }

    public JobAcceptedResponse submitJob(CreateJobRequest request) {
        validateSubmission(request);

        String normalizedKey = normalizeIdempotencyKey(request.idempotencyKey());
        if (normalizedKey != null) {
            Optional<JobEntity> existing = jobRepository.findByIdempotencyKey(normalizedKey);
            if (existing.isPresent()) {
                return toAcceptedResponse(existing.get(), true);
            }
        }

        Instant now = Instant.now();
        Instant scheduledFor = resolveScheduledTime(request, now);

        try {
            JobEntity created = jobWriteTxService.insertJobAndOutbox(request, normalizedKey, now, scheduledFor);
            if (!scheduledFor.isAfter(now)) {
                outboxRelayScheduler.triggerImmediateFlush();
            }
            return toAcceptedResponse(created, false);
        } catch (DataIntegrityViolationException ex) {
            if (normalizedKey != null) {
                return jobRepository.findByIdempotencyKey(normalizedKey)
                        .map(winner -> toAcceptedResponse(winner, true))
                        .orElseThrow(() -> ex);
            }
            throw ex;
        }
    }

    @Transactional(readOnly = true)
    public JobStatusResponse getJobStatus(String jobId) {
        JobEntity job = jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
        List<JobAttemptEntity> attempts =
                jobAttemptRepository.findByJobIdOrderByReplayGenerationAscAttemptNumberAsc(jobId);
        return toStatusResponse(job, attempts);
    }

    @Transactional(readOnly = true)
    public List<DeadLetterJobResponse> listDeadLetterJobs() {
        return deadLetterRecordRepository.findAllByOrderByDeadLetteredAtDesc()
                .stream()
                .map(this::toDeadLetterResponse)
                .toList();
    }

    @Transactional(readOnly = true)
    public DeadLetterJobResponse getDeadLetterJob(String jobId) {
        DeadLetterRecordEntity record = deadLetterRecordRepository
                .findFirstByJobIdOrderByDeadLetteredAtDesc(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
        return toDeadLetterResponse(record);
    }

    public JobAcceptedResponse replayDeadLetterJob(String jobId) {
        JobEntity replayed = jobWriteTxService.replayDeadLetteredJob(jobId);
        outboxRelayScheduler.triggerImmediateFlush();
        return toAcceptedResponse(replayed, false);
    }

    private void validateSubmission(CreateJobRequest request) {
        if (request.payload() == null || !request.payload().isObject()) {
            throw new IllegalArgumentException("payload must be a JSON object");
        }
        if (request.scheduledAt() != null && request.delayMs() != null && request.delayMs() > 0) {
            throw new IllegalArgumentException("Provide at most one of scheduledAt or delayMs");
        }
    }

    private Instant resolveScheduledTime(CreateJobRequest request, Instant now) {
        if (request.scheduledAt() != null) {
            return request.scheduledAt().isBefore(now) ? now : request.scheduledAt();
        }
        if (request.delayMs() != null && request.delayMs() > 0) {
            return now.plusMillis(request.delayMs());
        }
        return now;
    }

    private String normalizeIdempotencyKey(String key) {
        if (key == null) {
            return null;
        }
        String trimmed = key.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private JobAcceptedResponse toAcceptedResponse(JobEntity job, boolean deduplicated) {
        return new JobAcceptedResponse(
                job.getId(),
                job.getJobType(),
                job.getStatus(),
                job.getPriority(),
                job.getMaxRetries(),
                job.getTimeoutMs(),
                job.getSubmittedAt(),
                job.getScheduledFor(),
                job.getIdempotencyKey(),
                deduplicated,
                readMetadata(job.getMetadataJson()),
                "/api/v1/jobs/" + job.getId());
    }

    private JobStatusResponse toStatusResponse(JobEntity job, List<JobAttemptEntity> attemptEntities) {
        JsonNode resultNode = (job.getStatus() == JobStatus.SUCCEEDED && job.getResultJson() != null)
                ? readJsonNode(job.getResultJson())
                : null;

        JobStatusResponse.ErrorDetail lastError = (job.getLastErrorType() != null || job.getLastErrorMessage() != null)
                ? new JobStatusResponse.ErrorDetail(job.getLastErrorType(), job.getLastErrorMessage())
                : null;

        List<JobStatusResponse.AttemptSummary> attempts = attemptEntities.stream()
                .map(a -> new JobStatusResponse.AttemptSummary(
                        a.getAttemptNumber(),
                        a.getReplayGeneration(),
                        a.getWorkerId(),
                        a.getStatus(),
                        a.getStartedAt(),
                        a.getFinishedAt(),
                        a.getDurationMs(),
                        a.getErrorType(),
                        a.getErrorMessage(),
                        a.getRetryable()))
                .toList();

        return new JobStatusResponse(
                job.getId(),
                job.getJobType(),
                job.getStatus(),
                job.getPriority(),
                job.getAttemptCount(),
                job.getMaxRetries(),
                job.getTimeoutMs(),
                resultNode,
                lastError,
                job.getSubmittedAt(),
                job.getScheduledFor(),
                job.getStartedAt(),
                job.getFinishedAt(),
                job.getNextRetryAt(),
                job.getIdempotencyKey(),
                readMetadata(job.getMetadataJson()),
                job.getReplayCount(),
                attempts);
    }

    private DeadLetterJobResponse toDeadLetterResponse(DeadLetterRecordEntity entity) {
        return new DeadLetterJobResponse(
                entity.getId(),
                entity.getJobId(),
                entity.getJobType(),
                entity.getPriority(),
                readJsonNode(entity.getPayloadJson()),
                entity.getAttemptCount(),
                entity.getMaxRetries(),
                entity.getFailureReason(),
                entity.getErrorType(),
                entity.getErrorMessage(),
                entity.getStackTrace(),
                entity.getOriginalTopic(),
                entity.getDeadLetteredAt(),
                entity.isReplayed(),
                entity.getReplayedAt());
    }

    private JsonNode readJsonNode(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            return objectMapper.getNodeFactory().textNode(json);
        }
    }

    private Map<String, String> readMetadata(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, METADATA_TYPE);
        } catch (Exception e) {
            return Map.of();
        }
    }
}
