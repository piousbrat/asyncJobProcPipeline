package com.example.jobs.repository;

import com.example.jobs.domain.JobScheduleEntity;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface JobScheduleRepository extends JpaRepository<JobScheduleEntity, String> {

    boolean existsByName(String name);

    List<JobScheduleEntity> findAllByOrderByNameAsc();

    @Query(value = """
            SELECT * FROM job_schedules
            WHERE enabled = TRUE AND next_run_at <= :now
            ORDER BY next_run_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<JobScheduleEntity> lockDue(@Param("now") Instant now, @Param("limit") int limit);
}
