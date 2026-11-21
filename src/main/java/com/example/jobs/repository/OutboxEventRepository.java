package com.example.jobs.repository;

import com.example.jobs.domain.JobStatus;
import com.example.jobs.domain.OutboxEventEntity;
import com.example.jobs.domain.OutboxEventType;
import com.example.jobs.domain.OutboxStatus;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {

    @Query(value = """
            SELECT * FROM outbox_events
            WHERE (status = 'PENDING' AND available_at <= :now)
               OR (status = 'PROCESSING' AND lock_expires_at <= :now)
            ORDER BY priority_rank ASC, available_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockDueEvents(@Param("now") Instant now, @Param("limit") int limit);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE OutboxEventEntity e
            SET e.status = :cancelled, e.lockedBy = NULL, e.lockExpiresAt = NULL
            WHERE e.eventType = :dispatch AND e.status IN :open
              AND e.jobId IN (SELECT j.id FROM JobEntity j WHERE j.status = :failed AND j.lastErrorType = :errorType)
            """)
    int cancelDispatchesOfCancelledJobs(
            @Param("cancelled") OutboxStatus cancelled,
            @Param("dispatch") OutboxEventType dispatch,
            @Param("open") Collection<OutboxStatus> open,
            @Param("failed") JobStatus failed,
            @Param("errorType") String errorType);

    List<OutboxEventEntity> findByJobIdOrderByCreatedAtAsc(String jobId);

    long countByStatus(OutboxStatus status);
}
