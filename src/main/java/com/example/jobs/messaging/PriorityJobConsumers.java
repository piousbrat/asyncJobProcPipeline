package com.example.jobs.messaging;

import com.example.jobs.worker.JobExecutionEngine;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Priority-segregated Kafka consumers backed by dedicated container factories and thread concurrencies
 * so {@code HIGH} priority work is never starved behind {@code NORMAL} or {@code LOW} backlogs.
 */
@Component
public class PriorityJobConsumers {

    private static final Logger log = LoggerFactory.getLogger(PriorityJobConsumers.class);

    private final JobExecutionEngine executionEngine;
    private final ObjectMapper objectMapper;

    public PriorityJobConsumers(JobExecutionEngine executionEngine, ObjectMapper objectMapper) {
        this.executionEngine = executionEngine;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "jobs-high-listener",
            topics = "${app.jobs.topics.high}",
            containerFactory = "highPriorityListenerFactory")
    public void onHighPriority(ConsumerRecord<String, String> record) {
        handleRecord(record);
    }

    @KafkaListener(
            id = "jobs-normal-listener",
            topics = "${app.jobs.topics.normal}",
            containerFactory = "normalPriorityListenerFactory")
    public void onNormalPriority(ConsumerRecord<String, String> record) {
        handleRecord(record);
    }

    @KafkaListener(
            id = "jobs-low-listener",
            topics = "${app.jobs.topics.low}",
            containerFactory = "lowPriorityListenerFactory")
    public void onLowPriority(ConsumerRecord<String, String> record) {
        handleRecord(record);
    }

    private void handleRecord(ConsumerRecord<String, String> record) {
        JobDispatchEnvelope envelope;
        try {
            envelope = objectMapper.readValue(record.value(), JobDispatchEnvelope.class);
        } catch (Exception ex) {
            log.error("Discarding malformed JobDispatchEnvelope on topic={} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), ex.getMessage());
            return;
        }
        executionEngine.processDispatch(envelope, record.topic());
    }
}
