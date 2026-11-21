package com.example.jobs.web;

import com.example.jobs.config.JobPipelineProperties;
import com.example.jobs.service.RateLimiter;
import com.example.jobs.web.dto.ApiErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Per-client limit (see {@link ClientIdentity}) on the public API ({@code /api/v1/**} except internal
 * {@code /api/v1/ops/**}).
 */
@Component
public class RateLimitInterceptor implements HandlerInterceptor, WebMvcConfigurer {

    private final RateLimiter rateLimiter;
    private final JobPipelineProperties properties;
    private final ObjectMapper objectMapper;

    public RateLimitInterceptor(RateLimiter rateLimiter, JobPipelineProperties properties, ObjectMapper objectMapper) {
        this.rateLimiter = rateLimiter;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(this).addPathPatterns("/api/v1/**").excludePathPatterns("/api/v1/ops/**");
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        if (!properties.rateLimit().enabled()) {
            return true;
        }
        RateLimiter.Decision decision = rateLimiter.tryAcquire(ClientIdentity.of(request));
        response.setHeader("X-RateLimit-Limit", String.valueOf(decision.limit()));
        response.setHeader("X-RateLimit-Remaining", String.valueOf(decision.remaining()));
        if (decision.allowed()) {
            return true;
        }
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After", String.valueOf((decision.retryAfterMs() + 999) / 1000));
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getOutputStream(), new ApiErrorResponse(
                HttpStatus.TOO_MANY_REQUESTS.value(), HttpStatus.TOO_MANY_REQUESTS.getReasonPhrase(),
                "Rate limit of " + decision.limit() + " requests per " + properties.rateLimit().windowMs()
                        + " ms exceeded; retry after " + decision.retryAfterMs() + " ms",
                request.getRequestURI(), Instant.now(), null));
        return false;
    }
}
