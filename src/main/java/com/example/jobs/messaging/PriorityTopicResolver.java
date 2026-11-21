package com.example.jobs.messaging;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.domain.JobPriority;
import org.springframework.stereotype.Component;

@Component
public class PriorityTopicResolver {

    private final JobPipelineProperties properties;

    public PriorityTopicResolver(JobPipelineProperties properties) {
        this.properties = properties;
    }

    public String topicFor(JobPriority priority) {
        return switch (priority) {
            case HIGH -> properties.topics().high();
            case NORMAL -> properties.topics().normal();
            case LOW -> properties.topics().low();
        };
    }

    public String dlqTopic() {
        return properties.topics().dlq();
    }
}
