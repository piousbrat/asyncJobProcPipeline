package com.example.jobs.web;

import com.example.jobs.service.ScheduleService;
import com.example.jobs.web.dto.CreateScheduleRequest;
import com.example.jobs.web.dto.ScheduleResponse;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/schedules")
public class ScheduleController {

    private final ScheduleService scheduleService;

    public ScheduleController(ScheduleService scheduleService) {
        this.scheduleService = scheduleService;
    }

    @PostMapping
    public ResponseEntity<ScheduleResponse> create(@Valid @RequestBody CreateScheduleRequest request) {
        ScheduleResponse created = scheduleService.create(request);
        return ResponseEntity.created(URI.create("/api/v1/schedules/" + created.id())).body(created);
    }

    @GetMapping
    public List<ScheduleResponse> list() {
        return scheduleService.list();
    }

    @GetMapping("/{id}")
    public ScheduleResponse get(@PathVariable String id) {
        return scheduleService.get(id);
    }

    @PostMapping("/{id}/pause")
    public ScheduleResponse pause(@PathVariable String id) {
        return scheduleService.setEnabled(id, false);
    }

    @PostMapping("/{id}/resume")
    public ScheduleResponse resume(@PathVariable String id) {
        return scheduleService.setEnabled(id, true);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id) {
        scheduleService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
