package com.example.jobs.messaging;

import com.example.jobs.domain.DeadLetterRecordEntity;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.repository.DeadLetterRecordRepository;
import com.example.jobs.repository.JobRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consumes dead-letter events from {@code jobs.dlq}, logs structured diagnostics,
 * and idempotently records dead-lettered jobs for inspection and replay.
 */
@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    private final DeadLetterRecordRepository deadLetterRecordRepository;
    private final JobRepository jobRepository;
    private final ObjectMapper objectMapper;

    public DeadLetterConsumer(
            DeadLetterRecordRepository deadLetterRecordRepository,
            JobRepository jobRepository,
            ObjectMapper objectMapper) {
        this.deadLetterRecordRepository = deadLetterRecordRepository;
        this.jobRepository = jobRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "jobs-dlq-listener",
            topics = "${app.jobs.topics.dlq}",
            containerFactory = "dlqListenerFactory")
    @Transactional
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        DeadLetterEnvelope envelope;
        try {
            envelope = objectMapper.readValue(record.value(), DeadLetterEnvelope.class);
        } catch (Exception ex) {
            log.error("Received unparseable DLQ message on topic={} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), ex.getMessage());
            return;
        }

        if (envelope.outboxEventId() != null
                && deadLetterRecordRepository.findByOutboxEventId(envelope.outboxEventId()).isPresent()) {
            log.debug("Skipping duplicate DLQ delivery for outboxEventId={}", envelope.outboxEventId());
            return;
        }

        log.error("DEAD LETTERED JOB: jobId={}, type={}, priority={}, attempts={}/{}, reason={}, error={}: {}",
                envelope.jobId(),
                envelope.jobType(),
                envelope.priority(),
                envelope.attemptCount(),
                envelope.maxRetries(),
                envelope.failureReason(),
                envelope.errorType(),
                envelope.errorMessage());

        DeadLetterRecordEntity entity = new DeadLetterRecordEntity();
        entity.setId(UUID.randomUUID().toString());
        entity.setJobId(envelope.jobId());
        entity.setOutboxEventId(envelope.outboxEventId());
        entity.setJobType(envelope.jobType());
        entity.setPriority(envelope.priority());
        entity.setPayloadJson(envelope.payloadJson());
        entity.setAttemptCount(envelope.attemptCount());
        entity.setMaxRetries(envelope.maxRetries());
        entity.setFailureReason(envelope.failureReason());
        entity.setErrorType(envelope.errorType());
        entity.setErrorMessage(envelope.errorMessage());
        entity.setStackTrace(envelope.stackTrace());
        entity.setOriginalTopic(envelope.originalTopic());
        entity.setDeadLetteredAt(envelope.failedAt() != null ? envelope.failedAt() : Instant.now());
        entity.setReplayed(false);

        try {
            deadLetterRecordRepository.saveAndFlush(entity);
        } catch (DataIntegrityViolationException duplicate) {
            log.debug("Concurrent duplicate DLQ record ignored for outboxEventId={}", envelope.outboxEventId());
        }

        jobRepository.markDeadLettered(envelope.jobId(), JobStatus.FAILED, JobStatus.DEAD_LETTERED, Instant.now());
    }
}
