package com.example.jobs.outbox;

import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.OutboxEventEntity;
import com.example.jobs.domain.OutboxEventType;
import com.example.jobs.domain.OutboxStatus;
import com.example.jobs.messaging.JobDispatchEnvelope;
import com.example.jobs.messaging.PriorityTopicResolver;
import com.example.jobs.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** Builds the transactional-outbox row that dispatches an attempt of a job to its priority topic. */
@Component
public class OutboxEvents {

    private final OutboxEventRepository outboxEventRepository;
    private final PriorityTopicResolver topicResolver;
    private final ObjectMapper objectMapper;

    public OutboxEvents(
            OutboxEventRepository outboxEventRepository,
            PriorityTopicResolver topicResolver,
            ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.topicResolver = topicResolver;
        this.objectMapper = objectMapper;
    }

    public void enqueueDispatch(JobEntity job, int expectedAttempt, Instant availableAt, Instant now) {
        OutboxEventEntity event = new OutboxEventEntity();
        event.setId(UUID.randomUUID().toString());
        event.setJobId(job.getId());
        event.setEventType(OutboxEventType.JOB_DISPATCH);
        event.setTopic(topicResolver.topicFor(job.getPriority()));
        event.setPartitionKey(job.getId());
        event.setPriorityRank(job.getPriority().rank());
        event.setPayloadJson(toJson(new JobDispatchEnvelope(
                job.getId(), job.getJobType(), job.getPriority(),
                expectedAttempt, job.getReplayCount(), now)));
        event.setStatus(OutboxStatus.PENDING);
        event.setAvailableAt(availableAt);
        event.setCreatedAt(now);
        outboxEventRepository.save(event);
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }
}
