package com.example.jobs.service;

import com.example.jobs.backoff.RetryPolicy;
import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.exception.JobNotFoundException;
import com.example.jobs.exception.JobStateConflictException;
import com.example.jobs.outbox.OutboxEvents;
import com.example.jobs.repository.DeadLetterRecordRepository;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.web.dto.CreateJobRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobWriteTxService {

    private final JobRepository jobRepository;
    private final DeadLetterRecordRepository deadLetterRecordRepository;
    private final OutboxEvents outboxEvents;
    private final JobPipelineProperties properties;
    private final ObjectMapper objectMapper;

    public JobWriteTxService(
            JobRepository jobRepository,
            DeadLetterRecordRepository deadLetterRecordRepository,
            OutboxEvents outboxEvents,
            JobPipelineProperties properties,
            ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.deadLetterRecordRepository = deadLetterRecordRepository;
        this.outboxEvents = outboxEvents;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JobEntity insertJobAndOutbox(
            CreateJobRequest request,
            String normalizedIdempotencyKey,
            Instant now,
            Instant scheduledFor) {
        JobEntity job = new JobEntity();
        job.setId(UUID.randomUUID().toString());
        job.setIdempotencyKey(normalizedIdempotencyKey);
        job.setJobType(request.jobType().trim());
        job.setPriority(request.priority() != null ? request.priority() : JobPriority.NORMAL);
        job.setStatus(JobStatus.QUEUED);
        job.setPayloadJson(writeJson(request.payload()));
        job.setMetadataJson(request.metadata() != null ? writeJson(request.metadata()) : null);
        job.setMaxRetries(request.maxRetries() != null ? request.maxRetries() : RetryPolicy.MAX_RETRIES);
        job.setTimeoutMs(request.timeoutMs() != null ? request.timeoutMs() : properties.defaults().timeoutMs());
        job.setRouteToDlq(request.routeToDlq() == null || request.routeToDlq());
        job.setSubmittedAt(now);
        job.setScheduledFor(scheduledFor);
        job.setUpdatedAt(now);

        JobEntity saved = jobRepository.saveAndFlush(job);
        outboxEvents.enqueueDispatch(saved, 1, scheduledFor, now);
        return saved;
    }

    @Transactional
    public JobEntity replayDeadLetteredJob(String jobId) {
        JobEntity job = jobRepository.findById(jobId)
                .orElseThrow(() -> new JobNotFoundException(jobId));
        if (job.getStatus() != JobStatus.DEAD_LETTERED) {
            throw new JobStateConflictException(
                    "Only DEAD_LETTERED jobs can be replayed; current state is " + job.getStatus());
        }

        Instant now = Instant.now();
        job.setStatus(JobStatus.QUEUED);
        job.setAttemptCount(0);
        job.setReplayCount(job.getReplayCount() + 1);
        job.setScheduledFor(now);
        job.setStartedAt(null);
        job.setFinishedAt(null);
        job.setNextRetryAt(null);
        job.setResultJson(null);
        job.setLastErrorType(null);
        job.setLastErrorMessage(null);
        job.setLeaseOwner(null);
        job.setLeaseToken(null);
        job.setLeaseExpiresAt(null);
        job.setUpdatedAt(now);
        JobEntity saved = jobRepository.save(job);

        deadLetterRecordRepository.findFirstByJobIdOrderByDeadLetteredAtDesc(jobId).ifPresent(dlq -> {
            dlq.setReplayed(true);
            dlq.setReplayedAt(now);
            deadLetterRecordRepository.save(dlq);
        });

        outboxEvents.enqueueDispatch(saved, 1, now, now);
        return saved;
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JSON", e);
        }
    }
}
