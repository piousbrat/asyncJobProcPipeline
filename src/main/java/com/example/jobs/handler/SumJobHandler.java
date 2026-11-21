package com.example.jobs.handler;

import com.example.jobs.exception.InvalidPayloadException;
import com.example.jobs.worker.JobExecutionContext;
import com.example.jobs.worker.JobHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.Map;
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
    public Map<String, String> validatePayload(JsonNode payload) {
        JsonNode numbers = payload.get("numbers");
        if (numbers == null || !numbers.isArray() || numbers.isEmpty()) {
            return Map.of("numbers", "must be a non-empty array of numbers");
        }
        Map<String, String> errors = new LinkedHashMap<>();
        for (int i = 0; i < numbers.size(); i++) {
            if (!numbers.get(i).isNumber()) {
                errors.put("numbers[" + i + "]", "must be a number, got: " + numbers.get(i));
            }
        }
        return errors;
    }

    @Override
    public JsonNode execute(JobExecutionContext context) {
        InvalidPayloadException.throwIfAny(validatePayload(context.payload()));
        JsonNode numbers = context.payload().get("numbers");

        double sum = 0.0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        int count = 0;

        for (JsonNode element : numbers) {
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
