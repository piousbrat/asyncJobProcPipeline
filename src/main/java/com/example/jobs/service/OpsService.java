package com.example.jobs.service;

import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.domain.OutboxEventType;
import com.example.jobs.domain.OutboxStatus;
import com.example.jobs.exception.JobNotFoundException;
import com.example.jobs.exception.JobStateConflictException;
import com.example.jobs.repository.JobRepository;
import com.example.jobs.repository.OutboxEventRepository;
import com.example.jobs.web.dto.QueueDepthResponse;
import com.example.jobs.web.dto.QueueDepthResponse.Depth;
import java.time.Instant;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Operational controls. A cancelled job is a non-DLQ terminal failure: {@code FAILED} with
 * {@code lastError.type = JobCancelled}. Only QUEUED jobs can be cancelled; RUNNING attempts always finish.
 * Kafka messages already published for a cancelled job are ignored by the worker's conditional claim.
 */
@Service
public class OpsService {

    public static final String CANCELLED_ERROR_TYPE = "JobCancelled";
    private static final String CANCELLED_MESSAGE = "Cancelled by operator";

    private final JobRepository jobRepository;
    private final OutboxEventRepository outboxEventRepository;

    public OpsService(JobRepository jobRepository, OutboxEventRepository outboxEventRepository) {
        this.jobRepository = jobRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional(readOnly = true)
    public QueueDepthResponse queueDepth() {
        Map<JobPriority, Long> queued = new EnumMap<>(JobPriority.class);
        long running = 0;
        for (Object[] row : jobRepository.countByStatusAndPriority()) {
            if (row[0] == JobStatus.QUEUED) {
                queued.put((JobPriority) row[1], (Long) row[2]);
            } else if (row[0] == JobStatus.RUNNING) {
                running += (Long) row[2];
            }
        }
        Map<JobPriority, Long> delayed = new EnumMap<>(JobPriority.class);
        for (Object[] row : jobRepository.countDelayedByPriority(JobStatus.QUEUED, Instant.now())) {
            delayed.put((JobPriority) row[0], (Long) row[1]);
        }

        Map<JobPriority, Depth> depth = new EnumMap<>(JobPriority.class);
        for (JobPriority p : JobPriority.values()) {
            long total = queued.getOrDefault(p, 0L);
            long late = delayed.getOrDefault(p, 0L);
            depth.put(p, new Depth(total - late, late));
        }
        return new QueueDepthResponse(depth, running, outboxEventRepository.countByStatus(OutboxStatus.PENDING));
    }

    @Transactional
    public void cancel(String jobId) {
        int updated = jobRepository.cancelQueuedById(
                jobId, JobStatus.QUEUED, JobStatus.FAILED, CANCELLED_ERROR_TYPE, CANCELLED_MESSAGE, Instant.now());
        if (updated == 0) {
            JobStatus current = jobRepository.findById(jobId)
                    .orElseThrow(() -> new JobNotFoundException(jobId)).getStatus();
            throw new JobStateConflictException("Only QUEUED jobs can be cancelled; current state is " + current);
        }
        cancelPendingDispatches();
    }

    /** Cancels every QUEUED job (all priorities when {@code priority} is null) and returns how many. */
    @Transactional
    public int drain(JobPriority priority) {
        List<JobPriority> priorities = priority != null ? List.of(priority) : Arrays.asList(JobPriority.values());
        int cancelled = jobRepository.cancelQueued(
                priorities, JobStatus.QUEUED, JobStatus.FAILED, CANCELLED_ERROR_TYPE, CANCELLED_MESSAGE, Instant.now());
        cancelPendingDispatches();
        return cancelled;
    }

    private void cancelPendingDispatches() {
        outboxEventRepository.cancelDispatchesOfCancelledJobs(
                OutboxStatus.CANCELLED, OutboxEventType.JOB_DISPATCH,
                List.of(OutboxStatus.PENDING, OutboxStatus.PROCESSING), JobStatus.FAILED, CANCELLED_ERROR_TYPE);
    }
}
