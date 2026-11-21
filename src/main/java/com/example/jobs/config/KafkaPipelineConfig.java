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
        return TopicBuilder.name(props.getTopics().getHigh())
                .partitions(props.getTopics().getPartitions().getHigh())
                .replicas(props.getTopics().getReplicationFactor())
                .build();
    }

    @Bean
    public NewTopic normalPriorityTopic(JobPipelineProperties props) {
        return TopicBuilder.name(props.getTopics().getNormal())
                .partitions(props.getTopics().getPartitions().getNormal())
                .replicas(props.getTopics().getReplicationFactor())
                .build();
    }

    @Bean
    public NewTopic lowPriorityTopic(JobPipelineProperties props) {
        return TopicBuilder.name(props.getTopics().getLow())
                .partitions(props.getTopics().getPartitions().getLow())
                .replicas(props.getTopics().getReplicationFactor())
                .build();
    }

    @Bean
    public NewTopic dlqTopic(JobPipelineProperties props) {
        return TopicBuilder.name(props.getTopics().getDlq())
                .partitions(props.getTopics().getPartitions().getDlq())
                .replicas(props.getTopics().getReplicationFactor())
                .build();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> highPriorityListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.getConsumer().getHighConcurrency(),
                jobProps.getConsumer().getHighMaxPollRecords());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> normalPriorityListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.getConsumer().getNormalConcurrency(),
                jobProps.getConsumer().getNormalMaxPollRecords());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> lowPriorityListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.getConsumer().getLowConcurrency(),
                jobProps.getConsumer().getLowMaxPollRecords());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dlqListenerFactory(
            KafkaProperties kafkaProperties,
            JobPipelineProperties jobProps) {
        return buildFactory(
                kafkaProperties,
                jobProps.getConsumer().getDlqConcurrency(),
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
        return Executors.newFixedThreadPool(jobProps.getWorker().getExecutorPoolSize(), factory);
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
