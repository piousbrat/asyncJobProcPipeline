package com.example.jobs.service;

import com.example.jobs.domain.DeadLetterRecordEntity;
import com.example.jobs.domain.JobAttemptEntity;
import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.exception.JobNotFoundException;
import com.example.jobs.exception.RequestValidationException;
import com.example.jobs.outbox.OutboxRelayScheduler;
import com.example.jobs.repository.DeadLetterRecordRepository;
import com.example.jobs.repository.JobAttemptRepository;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.service.IdempotencyKeyResolver.ResolvedKey;
import com.example.jobs.web.dto.CreateJobRequest;
import com.example.jobs.web.dto.DeadLetterJobResponse;
import com.example.jobs.web.dto.JobAcceptedResponse;
import com.example.jobs.web.dto.JobStatusResponse;
import com.example.jobs.worker.JobHandlerRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
    private final IdempotencyCache idempotencyCache;
    private final IdempotencyKeyResolver keyResolver;
    private final JobHandlerRegistry jobHandlerRegistry;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public JobService(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository,
            DeadLetterRecordRepository deadLetterRecordRepository,
            JobWriteTxService jobWriteTxService,
            OutboxRelayScheduler outboxRelayScheduler,
            IdempotencyCache idempotencyCache,
            IdempotencyKeyResolver keyResolver,
            JobHandlerRegistry jobHandlerRegistry,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.deadLetterRecordRepository = deadLetterRecordRepository;
        this.jobWriteTxService = jobWriteTxService;
        this.outboxRelayScheduler = outboxRelayScheduler;
        this.idempotencyCache = idempotencyCache;
        this.keyResolver = keyResolver;
        this.jobHandlerRegistry = jobHandlerRegistry;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
    }

    public JobAcceptedResponse submitJob(CreateJobRequest request, String clientId, String eventId) {
        validateSubmission(request);

        Instant now = Instant.now();
        ResolvedKey key = keyResolver.resolve(request, clientId, eventId, now);
        String claimToken = UUID.randomUUID().toString();
        if (key != null) {
            Optional<JobAcceptedResponse> existing = idempotencyCache.claimOrGetExisting(key.cacheKey(), claimToken);
            if (existing.isPresent()) {
                countDeduplicated("redis", key);
                return existing.get();
            }
        }

        Instant scheduledFor = resolveScheduledTime(request, now);
        String dbKey = key != null ? key.dbKey() : null;

        JobAcceptedResponse response;
        try {
            response = toAcceptedResponse(
                    jobWriteTxService.insertJobAndOutbox(request, dbKey, now, scheduledFor), false);
        } catch (DataIntegrityViolationException ex) {
            // Redis lost the key (eviction, failover, outage): the DB constraint is the source of truth.
            Optional<JobEntity> winner = dbKey != null ? jobRepository.findByIdempotencyKey(dbKey) : Optional.empty();
            if (winner.isEmpty() && request.jobId() != null) {
                winner = jobRepository.findById(request.jobId());
            }
            if (winner.isEmpty()) {
                releaseClaim(key, claimToken);
                throw ex;
            }
            countDeduplicated("database", key);
            response = toAcceptedResponse(winner.get(), true);
        } catch (RuntimeException ex) {
            releaseClaim(key, claimToken);
            throw ex;
        }

        if (key != null) {
            idempotencyCache.complete(key.cacheKey(), response, key.cacheTtlMs());
        }
        if (!response.deduplicated() && !scheduledFor.isAfter(now)) {
            outboxRelayScheduler.triggerImmediateFlush();
        }
        return response;
    }

    private void releaseClaim(ResolvedKey key, String claimToken) {
        if (key != null) {
            idempotencyCache.release(key.cacheKey(), claimToken);
        }
    }

    private void countDeduplicated(String source, ResolvedKey key) {
        meterRegistry.counter("jobs.submissions.deduplicated",
                "source", source, "key", key.source().name().toLowerCase()).increment();
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
        jobHandlerRegistry.requireSupported("jobType", request.jobType());
        if (request.payload() == null || !request.payload().isObject()) {
            throw new RequestValidationException("payload", "payload must be a JSON object");
        }
        if (request.scheduledAt() != null && request.delayMs() != null && request.delayMs() > 0) {
            throw new RequestValidationException("delayMs", "Provide at most one of scheduledAt or delayMs");
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
