package com.example.jobs.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaPipelineConfig {

    @Bean
    public NewTopic highPriorityTopic(JobPipelineProperties props) {
        return TopicBuilder.name(props.topics().high())
                .partitions(props.topics().partitions().high())
                .replicas(props.topics().replicationFactor())
                .build();
    }

    @Bean
    public NewTopic normalPriorityTopic(JobPipelineProperties props) {
        return TopicBuilder.name(props.topics().normal())
                .partitions(props.topics().partitions().normal())
                .replicas(props.topics().replicationFactor())
                .build();
    }

    @Bean
    public NewTopic lowPriorityTopic(JobPipelineProperties props) {
        return TopicBuilder.name(props.topics().low())
                .partitions(props.topics().partitions().low())
                .replicas(props.topics().replicationFactor())
                .build();
    }

    @Bean
    public NewTopic dlqTopic(JobPipelineProperties props) {
        return TopicBuilder.name(props.topics().dlq())
                .partitions(props.topics().partitions().dlq())
                .replicas(props.topics().replicationFactor())
                .build();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> highPriorityListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.consumer().highConcurrency(),
                jobProps.consumer().highMaxPollRecords());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> normalPriorityListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.consumer().normalConcurrency(),
                jobProps.consumer().normalMaxPollRecords());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> lowPriorityListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.consumer().lowConcurrency(),
                jobProps.consumer().lowMaxPollRecords());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dlqListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.consumer().dlqConcurrency(),
                10);
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService jobAttemptExecutor(JobPipelineProperties jobProps) {
        AtomicInteger counter = new AtomicInteger(1);
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "job-attempt-exec-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newFixedThreadPool(jobProps.worker().executorPoolSize(), factory);
    }

    private ConcurrentKafkaListenerContainerFactory<String, String> buildFactory(
            KafkaProperties kafkaProperties,
            int concurrency,
            int maxPollRecords) {
        Map<String, Object> consumerProps = new HashMap<>(kafkaProperties.buildConsumerProperties());
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // Job retries are scheduled durably via the non-blocking outbox; only brief broker/DB hiccups retry in-container.
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(100L, 2L)));
        return factory;
    }
}
