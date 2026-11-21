package com.example.jobs.handler;

import com.example.jobs.worker.JobExecutionContext;
import com.example.jobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

/**
 * General-purpose handler useful for exercising delays, timeouts, transient retries,
 * non-DLQ terminal failures, and DLQ routing via payload simulation directives.
 */
@Component
public class SimulationJobHandler implements JobHandler {

    private final ObjectMapper objectMapper;

    public SimulationJobHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String jobType() {
        return "simulate";
    }

    @Override
    public JsonNode execute(JobExecutionContext context) {
        ObjectNode result = objectMapper.createObjectNode();
        result.put("simulated", true);
        result.put("attempt", context.attemptNumber());
        result.put("priority", context.priority().name());
        if (context.payload().has("resultValue")) {
            result.set("resultValue", context.payload().get("resultValue"));
        }
        return result;
    }
}
