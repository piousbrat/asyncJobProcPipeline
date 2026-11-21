package com.example.jobs.outbox;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.domain.OutboxEventEntity;
import com.example.jobs.domain.OutboxEventType;
import com.example.jobs.domain.OutboxStatus;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.OutboxEventRepository;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxTxService {

    private final OutboxEventRepository outboxEventRepository;
    private final JobRepository jobRepository;
    private final JobPipelineProperties properties;

    public OutboxTxService(
            OutboxEventRepository outboxEventRepository,
            JobRepository jobRepository,
            JobPipelineProperties properties) {
        this.outboxEventRepository = outboxEventRepository;
        this.jobRepository = jobRepository;
        this.properties = properties;
    }

    @Transactional
    public List<OutboxEventEntity> claimDueBatch(Instant now, int batchSize) {
        List<OutboxEventEntity> locked = outboxEventRepository.lockDueEvents(now, batchSize);
        if (locked.isEmpty()) {
            return List.of();
        }
        Instant lockExpiresAt = now.plusMillis(properties.outbox().lockDurationMs());
        for (OutboxEventEntity event : locked) {
            event.setStatus(OutboxStatus.PROCESSING);
            event.setLockedBy(properties.workerId());
            event.setLockExpiresAt(lockExpiresAt);
            event.setPublishAttempts(event.getPublishAttempts() + 1);
        }
        return outboxEventRepository.saveAll(locked);
    }

    @Transactional
    public void markPublished(String eventId, String jobId, OutboxEventType eventType, Instant publishedAt) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            event.setStatus(OutboxStatus.PUBLISHED);
            event.setPublishedAt(publishedAt);
            event.setLockedBy(null);
            event.setLockExpiresAt(null);
            event.setLastError(null);
            outboxEventRepository.save(event);
        });

        if (eventType == OutboxEventType.JOB_DLQ) {
            jobRepository.markDeadLettered(jobId, JobStatus.FAILED, JobStatus.DEAD_LETTERED, publishedAt);
        }
    }

    @Transactional
    public void releaseOnFailure(String eventId, String errorMessage, Instant nextAvailableAt) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            event.setStatus(OutboxStatus.PENDING);
            event.setAvailableAt(nextAvailableAt);
            event.setLockedBy(null);
            event.setLockExpiresAt(null);
            event.setLastError(errorMessage);
            outboxEventRepository.save(event);
        });
    }
}
