package com.example.jobs.handler;

import com.example.jobs.exception.InvalidPayloadException;
import com.example.jobs.worker.JobExecutionContext;
import com.example.jobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class SumJobHandler implements JobHandler {

    private final ObjectMapper objectMapper;

    public SumJobHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String jobType() {
        return "sum";
    }

    @Override
    public JsonNode execute(JobExecutionContext context) {
        JsonNode numbers = context.payload().get("numbers");
        if (numbers == null || !numbers.isArray() || numbers.isEmpty()) {
            throw new InvalidPayloadException("sum job requires a non-empty numeric 'numbers' array in payload");
        }

        double sum = 0.0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        int count = 0;

        for (JsonNode element : numbers) {
            if (element == null || !element.isNumber()) {
                throw new InvalidPayloadException("All elements in 'numbers' must be numeric, got: " + element);
            }
            double value = element.doubleValue();
            sum += value;
            min = Math.min(min, value);
            max = Math.max(max, value);
            count++;
        }

        ObjectNode result = objectMapper.createObjectNode();
        result.put("sum", sum);
        result.put("count", count);
        result.put("min", min);
        result.put("max", max);
        result.put("average", sum / count);
        return result;
    }
}
