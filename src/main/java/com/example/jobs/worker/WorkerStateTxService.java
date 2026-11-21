package com.example.jobs.worker;

import com.example.jobs.backoff.JitteredExponentialBackoffCalculator;
import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.domain.AttemptStatus;
import com.example.jobs.domain.JobAttemptEntity;
import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.domain.OutboxEventEntity;
import com.example.jobs.domain.OutboxEventType;
import com.example.jobs.domain.OutboxStatus;
import com.example.jobs.exception.NonRetryableJobException;
import com.example.jobs.exception.TerminalJobFailureException;
import com.example.jobs.messaging.DeadLetterEnvelope;
import com.example.jobs.messaging.JobDispatchEnvelope;
import com.example.jobs.messaging.PriorityTopicResolver;
import com.example.jobs.outbox.OutboxRelayScheduler;
import com.example.jobs.repository.JobAttemptRepository;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Manages atomic state transitions, fencing-token verification, non-blocking retry scheduling,
 * and dead-letter outbox creation inside database transactions.
 */
@Service
public class WorkerStateTxService {

    private static final Logger log = LoggerFactory.getLogger(WorkerStateTxService.class);
    private static final int MAX_STACK_TRACE_CHARS = 4000;

    private final JobRepository jobRepository;
    private final JobAttemptRepository jobAttemptRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final PriorityTopicResolver topicResolver;
    private final JitteredExponentialBackoffCalculator backoffCalculator;
    private final OutboxRelayScheduler outboxRelayScheduler;
    private final JobPipelineProperties properties;
    private final ObjectMapper objectMapper;

    public WorkerStateTxService(
            JobRepository jobRepository,
            JobAttemptRepository jobAttemptRepository,
            OutboxEventRepository outboxEventRepository,
            PriorityTopicResolver topicResolver,
            JitteredExponentialBackoffCalculator backoffCalculator,
            OutboxRelayScheduler outboxRelayScheduler,
            JobPipelineProperties properties,
            ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.jobAttemptRepository = jobAttemptRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.topicResolver = topicResolver;
        this.backoffCalculator = backoffCalculator;
        this.outboxRelayScheduler = outboxRelayScheduler;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Optional<ClaimedJobAttempt> tryClaimAttempt(JobDispatchEnvelope envelope) {
        int expectedPriorAttempt = envelope.expectedAttempt() - 1;
        if (expectedPriorAttempt < 0) {
            return Optional.empty();
        }

        Optional<JobEntity> peek = jobRepository.findById(envelope.jobId());
        if (peek.isEmpty()) {
            log.warn("Received dispatch for unknown jobId={}", envelope.jobId());
            return Optional.empty();
        }

        JobEntity existing = peek.get();
        if (existing.getStatus() != JobStatus.QUEUED
                || existing.getAttemptCount() != expectedPriorAttempt
                || existing.getReplayCount() != envelope.replayGeneration()) {
            log.debug("Skipping duplicate/stale dispatch for jobId={} (status={}, attemptCount={}, expectedAttempt={})",
                    envelope.jobId(), existing.getStatus(), existing.getAttemptCount(), envelope.expectedAttempt());
            return Optional.empty();
        }

        Instant now = Instant.now();
        String leaseToken = UUID.randomUUID().toString();
        long leaseDurationMs = existing.getTimeoutMs() + properties.getWorker().getLeaseGraceMs();
        Instant leaseExpiresAt = now.plusMillis(leaseDurationMs);

        int updated = jobRepository.claimQueuedAttempt(
                envelope.jobId(),
                JobStatus.QUEUED,
                JobStatus.RUNNING,
                expectedPriorAttempt,
                envelope.replayGeneration(),
                properties.getWorkerId(),
                leaseToken,
                leaseExpiresAt,
                now);

        if (updated == 0) {
            log.debug("Lost race to claim jobId={} expectedAttempt={}", envelope.jobId(), envelope.expectedAttempt());
            return Optional.empty();
        }

        JobEntity claimedJob = jobRepository.findById(envelope.jobId()).orElseThrow();

        JobAttemptEntity attempt = new JobAttemptEntity();
        attempt.setId(UUID.randomUUID().toString());
        attempt.setJobId(claimedJob.getId());
        attempt.setAttemptNumber(claimedJob.getAttemptCount());
        attempt.setReplayGeneration(claimedJob.getReplayCount());
        attempt.setWorkerId(properties.getWorkerId());
        attempt.setStatus(AttemptStatus.RUNNING);
        attempt.setStartedAt(now);
        jobAttemptRepository.save(attempt);

        return Optional.of(new ClaimedJobAttempt(claimedJob, attempt.getId(), leaseToken, now));
    }

    @Transactional
    public boolean renewLease(String jobId, String leaseToken, long timeoutMs) {
        Instant now = Instant.now();
        Instant extended = now.plusMillis(timeoutMs + properties.getWorker().getLeaseGraceMs());
        return jobRepository.extendLease(jobId, JobStatus.RUNNING, leaseToken, extended, now) > 0;
    }

    @Transactional
    public boolean completeSuccess(ClaimedJobAttempt claimed, JsonNode resultPayload) {
        JobEntity job = jobRepository.findById(claimed.job().getId()).orElse(null);
        if (job == null || job.getStatus() != JobStatus.RUNNING || !claimed.leaseToken().equals(job.getLeaseToken())) {
            log.warn("Fenced stale completion for jobId={} leaseToken={}",
                    claimed.job().getId(), claimed.leaseToken());
            return false;
        }

        Instant now = Instant.now();
        JsonNode nonNullResult = resultPayload != null ? resultPayload : objectMapper.createObjectNode();

        job.setStatus(JobStatus.SUCCEEDED);
        job.setResultJson(writeJson(nonNullResult));
        job.setFinishedAt(now);
        job.setNextRetryAt(null);
        clearLease(job);
        job.setUpdatedAt(now);
        jobRepository.save(job);

        jobAttemptRepository.findById(claimed.attemptEntityId()).ifPresent(attempt -> {
            attempt.setStatus(AttemptStatus.SUCCEEDED);
            attempt.setFinishedAt(now);
            attempt.setDurationMs(Math.max(0L, Duration.between(attempt.getStartedAt(), now).toMillis()));
            jobAttemptRepository.save(attempt);
        });

        log.info("Job {} SUCCEEDED on attempt {}", job.getId(), job.getAttemptCount());
        return true;
    }

    @Transactional
    public void handleAttemptFailure(
            ClaimedJobAttempt claimed,
            Throwable failure,
            boolean timedOut,
            String sourceTopic) {
        JobEntity job = jobRepository.findById(claimed.job().getId()).orElse(null);
        if (job == null || job.getStatus() != JobStatus.RUNNING || !claimed.leaseToken().equals(job.getLeaseToken())) {
            log.warn("Fenced stale failure transition for jobId={} leaseToken={}",
                    claimed.job().getId(), claimed.leaseToken());
            return;
        }

        Instant now = Instant.now();
        boolean nonDlqTerminal = failure instanceof TerminalJobFailureException;
        boolean nonRetryable = nonDlqTerminal || (failure instanceof NonRetryableJobException);
        boolean retryable = !nonRetryable;

        String errorType = failure.getClass().getName();
        String errorMessage = failure.getMessage() != null ? failure.getMessage() : failure.getClass().getSimpleName();

        jobAttemptRepository.findById(claimed.attemptEntityId()).ifPresent(attempt -> {
            attempt.setStatus(timedOut ? AttemptStatus.TIMED_OUT : AttemptStatus.FAILED);
            attempt.setFinishedAt(now);
            attempt.setDurationMs(Math.max(0L, Duration.between(attempt.getStartedAt(), now).toMillis()));
            attempt.setErrorType(errorType);
            attempt.setErrorMessage(errorMessage);
            attempt.setRetryable(retryable);
            jobAttemptRepository.save(attempt);
        });

        applyFailureOutcome(job, failure, errorType, errorMessage, retryable, nonDlqTerminal, sourceTopic, now);
    }

    @Transactional
    public int recoverExpiredRunningJobs(Instant now, int batchSize) {
        List<JobEntity> expiredJobs = jobRepository.lockExpiredRunningJobs(now, batchSize);
        if (expiredJobs.isEmpty()) {
            return 0;
        }

        for (JobEntity job : expiredJobs) {
            RuntimeException leaseExpiredEx = new IllegalStateException(
                    "Worker lease expired for owner=" + job.getLeaseOwner());
            String errorType = leaseExpiredEx.getClass().getName();
            String errorMessage = leaseExpiredEx.getMessage();

            jobAttemptRepository.findByJobIdAndReplayGenerationAndAttemptNumber(
                    job.getId(), job.getReplayCount(), job.getAttemptCount())
                    .ifPresent(attempt -> {
                        if (attempt.getStatus() == AttemptStatus.RUNNING) {
                            attempt.setStatus(AttemptStatus.TIMED_OUT);
                            attempt.setFinishedAt(now);
                            attempt.setDurationMs(Math.max(0L, Duration.between(attempt.getStartedAt(), now).toMillis()));
                            attempt.setErrorType(errorType);
                            attempt.setErrorMessage(errorMessage);
                            attempt.setRetryable(true);
                            jobAttemptRepository.save(attempt);
                        }
                    });

            log.warn("Recovering expired RUNNING lease on job {} (attemptCount={}, maxRetries={})",
                    job.getId(), job.getAttemptCount(), job.getMaxRetries());

            applyFailureOutcome(
                    job,
                    leaseExpiredEx,
                    errorType,
                    errorMessage,
                    true,
                    false,
                    topicResolver.topicFor(job.getPriority()),
                    now);
        }
        return expiredJobs.size();
    }

    private void applyFailureOutcome(
            JobEntity job,
            Throwable failure,
            String errorType,
            String errorMessage,
            boolean retryable,
            boolean nonDlqTerminal,
            String sourceTopic,
            Instant now) {
        job.setLastErrorType(errorType);
        job.setLastErrorMessage(errorMessage);
        clearLease(job);
        job.setUpdatedAt(now);

        if (retryable && job.getAttemptCount() <= job.getMaxRetries()) {
            int retryNumber = job.getAttemptCount();
            long backoffMs = backoffCalculator.computeDelayMs(retryNumber);
            Instant nextRetryAt = now.plusMillis(backoffMs);

            job.setStatus(JobStatus.QUEUED);
            job.setNextRetryAt(nextRetryAt);
            job.setFinishedAt(null);
            jobRepository.save(job);

            int nextAttempt = job.getAttemptCount() + 1;
            JobDispatchEnvelope retryEnvelope = new JobDispatchEnvelope(
                    job.getId(),
                    job.getJobType(),
                    job.getPriority(),
                    nextAttempt,
                    job.getReplayCount(),
                    now);

            OutboxEventEntity retryOutbox = new OutboxEventEntity();
            retryOutbox.setId(UUID.randomUUID().toString());
            retryOutbox.setJobId(job.getId());
            retryOutbox.setEventType(OutboxEventType.JOB_DISPATCH);
            retryOutbox.setTopic(topicResolver.topicFor(job.getPriority()));
            retryOutbox.setPartitionKey(job.getId());
            retryOutbox.setPriorityRank(job.getPriority().rank());
            retryOutbox.setPayloadJson(writeJson(retryEnvelope));
            retryOutbox.setHeadersJson(writeJson(Map.of(
                    "x-job-id", job.getId(),
                    "x-job-type", job.getJobType(),
                    "x-job-priority", job.getPriority().name(),
                    "x-expected-attempt", String.valueOf(nextAttempt))));
            retryOutbox.setStatus(OutboxStatus.PENDING);
            retryOutbox.setAvailableAt(nextRetryAt);
            retryOutbox.setPublishAttempts(0);
            retryOutbox.setCreatedAt(now);
            outboxEventRepository.save(retryOutbox);

            log.info("Scheduled retry #{} (attempt {}) for job {} in {}ms at {} ({}: {})",
                    retryNumber, nextAttempt, job.getId(), backoffMs, nextRetryAt, errorType, errorMessage);
            return;
        }

        // Terminal failure: either stays FAILED (non-DLQ terminal) or publishes to DLQ -> DEAD_LETTERED.
        job.setStatus(JobStatus.FAILED);
        job.setFinishedAt(now);
        job.setNextRetryAt(null);
        jobRepository.save(job);

        if (nonDlqTerminal || !job.isRouteToDlq()) {
            log.warn("Job {} entered terminal FAILED state without DLQ routing ({}: {})",
                    job.getId(), errorType, errorMessage);
            return;
        }

        String failureReason = retryable ? "MAX_RETRIES_EXHAUSTED" : "NON_RETRYABLE";
        String outboxEventId = UUID.randomUUID().toString();
        String stackTrace = summarizeStackTrace(failure);

        DeadLetterEnvelope dlqEnvelope = new DeadLetterEnvelope(
                outboxEventId,
                job.getId(),
                job.getJobType(),
                job.getPriority(),
                job.getPayloadJson(),
                job.getAttemptCount(),
                job.getMaxRetries(),
                failureReason,
                errorType,
                errorMessage,
                stackTrace,
                sourceTopic,
                now);

        Map<String, String> dlqHeaders = new LinkedHashMap<>();
        dlqHeaders.put("x-job-id", job.getId());
        dlqHeaders.put("x-job-type", job.getJobType());
        dlqHeaders.put("x-job-priority", job.getPriority().name());
        dlqHeaders.put("x-attempt-count", String.valueOf(job.getAttemptCount()));
        dlqHeaders.put("x-max-retries", String.valueOf(job.getMaxRetries()));
        dlqHeaders.put("x-failure-reason", failureReason);
        dlqHeaders.put("x-error-type", errorType);
        dlqHeaders.put("x-error-message", truncate(errorMessage, 512));
        dlqHeaders.put("x-original-topic", sourceTopic);

        OutboxEventEntity dlqOutbox = new OutboxEventEntity();
        dlqOutbox.setId(outboxEventId);
        dlqOutbox.setJobId(job.getId());
        dlqOutbox.setEventType(OutboxEventType.JOB_DLQ);
        dlqOutbox.setTopic(topicResolver.dlqTopic());
        dlqOutbox.setPartitionKey(job.getId());
        dlqOutbox.setPriorityRank(0);
        dlqOutbox.setPayloadJson(writeJson(dlqEnvelope));
        dlqOutbox.setHeadersJson(writeJson(dlqHeaders));
        dlqOutbox.setStatus(OutboxStatus.PENDING);
        dlqOutbox.setAvailableAt(now);
        dlqOutbox.setPublishAttempts(0);
        dlqOutbox.setCreatedAt(now);
        outboxEventRepository.save(dlqOutbox);

        registerPostCommitFlush();
        log.error("Job {} queued for DLQ (reason={}, attempts={}, maxRetries={}, error={}: {})",
                job.getId(), failureReason, job.getAttemptCount(), job.getMaxRetries(), errorType, errorMessage);
    }

    private void registerPostCommitFlush() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    outboxRelayScheduler.triggerImmediateFlush();
                }
            });
        } else {
            outboxRelayScheduler.triggerImmediateFlush();
        }
    }

    private void clearLease(JobEntity job) {
        job.setLeaseOwner(null);
        job.setLeaseToken(null);
        job.setLeaseExpiresAt(null);
    }

    private String summarizeStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        return truncate(sw.toString(), MAX_STACK_TRACE_CHARS);
    }

    private String truncate(String input, int maxLen) {
        if (input == null || input.length() <= maxLen) {
            return input;
        }
        return input.substring(0, maxLen) + "...";
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JSON", e);
        }
    }
}
