package com.example.jobs;

import com.example.jobs.config.JobPipelineProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(JobPipelineProperties.class)
public class AsyncJobPipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(AsyncJobPipelineApplication.class, args);
    }
}
