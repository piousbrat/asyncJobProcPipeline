package com.example.jobs.service;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.domain.DeadLetterRecordEntity;
import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.domain.OutboxEventEntity;
import com.example.jobs.domain.OutboxEventType;
import com.example.jobs.domain.OutboxStatus;
import com.example.jobs.exception.JobNotFoundException;
import com.example.jobs.exception.JobStateConflictException;
import com.example.jobs.messaging.JobDispatchEnvelope;
import com.example.jobs.messaging.PriorityTopicResolver;
import com.example.jobs.repository.DeadLetterRecordRepository;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.OutboxEventRepository;
import com.example.jobs.web.dto.CreateJobRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobWriteTxService {

    private final JobRepository jobRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final DeadLetterRecordRepository deadLetterRecordRepository;
    private final PriorityTopicResolver topicResolver;
    private final JobPipelineProperties properties;
    private final ObjectMapper objectMapper;

    public JobWriteTxService(
            JobRepository jobRepository,
            OutboxEventRepository outboxEventRepository,
            DeadLetterRecordRepository deadLetterRecordRepository,
            PriorityTopicResolver topicResolver,
            JobPipelineProperties properties,
            ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.deadLetterRecordRepository = deadLetterRecordRepository;
        this.topicResolver = topicResolver;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JobEntity insertJobAndOutbox(
            CreateJobRequest request,
            String normalizedIdempotencyKey,
            Instant now,
            Instant scheduledFor) {
        String jobId = UUID.randomUUID().toString();
        JobPriority priority = request.priority() != null ? request.priority() : JobPriority.NORMAL;
        int maxRetries = request.maxRetries() != null
                ? request.maxRetries()
                : properties.getDefaults().getMaxRetries();
        long timeoutMs = request.timeoutMs() != null
                ? request.timeoutMs()
                : properties.getDefaults().getTimeoutMs();
        boolean routeToDlq = request.routeToDlq() == null || request.routeToDlq();

        JobEntity job = new JobEntity();
        job.setId(jobId);
        job.setIdempotencyKey(normalizedIdempotencyKey);
        job.setJobType(request.jobType().trim());
        job.setPriority(priority);
        job.setStatus(JobStatus.QUEUED);
        job.setPayloadJson(writeJson(request.payload()));
        job.setMetadataJson(request.metadata() != null ? writeJson(request.metadata()) : null);
        job.setResultJson(null);
        job.setAttemptCount(0);
        job.setMaxRetries(maxRetries);
        job.setTimeoutMs(timeoutMs);
        job.setRouteToDlq(routeToDlq);
        job.setSubmittedAt(now);
        job.setScheduledFor(scheduledFor);
        job.setReplayCount(0);
        job.setUpdatedAt(now);

        JobEntity saved = jobRepository.saveAndFlush(job);

        JobDispatchEnvelope dispatchEnvelope = new JobDispatchEnvelope(
                jobId,
                saved.getJobType(),
                priority,
                1,
                0,
                now);

        OutboxEventEntity outbox = new OutboxEventEntity();
        outbox.setId(UUID.randomUUID().toString());
        outbox.setJobId(jobId);
        outbox.setEventType(OutboxEventType.JOB_DISPATCH);
        outbox.setTopic(topicResolver.topicFor(priority));
        outbox.setPartitionKey(jobId);
        outbox.setPriorityRank(priority.rank());
        outbox.setPayloadJson(writeJson(dispatchEnvelope));
        outbox.setHeadersJson(writeJson(Map.of(
                "x-job-id", jobId,
                "x-job-type", saved.getJobType(),
                "x-job-priority", priority.name(),
                "x-expected-attempt", "1")));
        outbox.setStatus(OutboxStatus.PENDING);
        outbox.setAvailableAt(scheduledFor);
        outbox.setPublishAttempts(0);
        outbox.setCreatedAt(now);

        outboxEventRepository.save(outbox);
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
        int nextReplayGeneration = job.getReplayCount() + 1;

        job.setStatus(JobStatus.QUEUED);
        job.setAttemptCount(0);
        job.setReplayCount(nextReplayGeneration);
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

        deadLetterRecordRepository.findFirstByJobIdOrderByDeadLetteredAtDesc(jobId)
                .ifPresent(dlq -> {
                    dlq.setReplayed(true);
                    dlq.setReplayedAt(now);
                    deadLetterRecordRepository.save(dlq);
                });

        JobDispatchEnvelope dispatchEnvelope = new JobDispatchEnvelope(
                jobId,
                saved.getJobType(),
                saved.getPriority(),
                1,
                nextReplayGeneration,
                now);

        OutboxEventEntity outbox = new OutboxEventEntity();
        outbox.setId(UUID.randomUUID().toString());
        outbox.setJobId(jobId);
        outbox.setEventType(OutboxEventType.JOB_DISPATCH);
        outbox.setTopic(topicResolver.topicFor(saved.getPriority()));
        outbox.setPartitionKey(jobId);
        outbox.setPriorityRank(saved.getPriority().rank());
        outbox.setPayloadJson(writeJson(dispatchEnvelope));
        outbox.setHeadersJson(writeJson(Map.of(
                "x-job-id", jobId,
                "x-job-type", saved.getJobType(),
                "x-job-priority", saved.getPriority().name(),
                "x-expected-attempt", "1",
                "x-replay-generation", String.valueOf(nextReplayGeneration))));
        outbox.setStatus(OutboxStatus.PENDING);
        outbox.setAvailableAt(now);
        outbox.setPublishAttempts(0);
        outbox.setCreatedAt(now);

        outboxEventRepository.save(outbox);
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
