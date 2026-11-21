package com.example.jobs.repository;

import com.example.jobs.domain.JobEntity;
import com.example.jobs.domain.JobPriority;
import com.example.jobs.domain.JobStatus;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface JobRepository extends JpaRepository<JobEntity, String> {

    Optional<JobEntity> findByIdempotencyKey(String idempotencyKey);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE JobEntity j
            SET j.status = :runningStatus,
                j.attemptCount = j.attemptCount + 1,
                j.startedAt = COALESCE(j.startedAt, :now),
                j.nextRetryAt = NULL,
                j.leaseOwner = :workerId,
                j.leaseToken = :leaseToken,
                j.leaseExpiresAt = :leaseExpiresAt,
                j.updatedAt = :now,
                j.version = j.version + 1
            WHERE j.id = :jobId
              AND j.status = :queuedStatus
              AND j.attemptCount = :expectedPriorAttempt
              AND j.replayCount = :expectedReplayCount
            """)
    int claimQueuedAttempt(
            @Param("jobId") String jobId,
            @Param("queuedStatus") JobStatus queuedStatus,
            @Param("runningStatus") JobStatus runningStatus,
            @Param("expectedPriorAttempt") int expectedPriorAttempt,
            @Param("expectedReplayCount") int expectedReplayCount,
            @Param("workerId") String workerId,
            @Param("leaseToken") String leaseToken,
            @Param("leaseExpiresAt") Instant leaseExpiresAt,
            @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE JobEntity j
            SET j.leaseExpiresAt = :newLeaseExpiresAt,
                j.updatedAt = :now
            WHERE j.id = :jobId
              AND j.status = :runningStatus
              AND j.leaseToken = :leaseToken
            """)
    int extendLease(
            @Param("jobId") String jobId,
            @Param("runningStatus") JobStatus runningStatus,
            @Param("leaseToken") String leaseToken,
            @Param("newLeaseExpiresAt") Instant newLeaseExpiresAt,
            @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE JobEntity j
            SET j.status = :deadLetteredStatus,
                j.updatedAt = :now,
                j.version = j.version + 1
            WHERE j.id = :jobId
              AND j.status = :failedStatus
            """)
    int markDeadLettered(
            @Param("jobId") String jobId,
            @Param("failedStatus") JobStatus failedStatus,
            @Param("deadLetteredStatus") JobStatus deadLetteredStatus,
            @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE JobEntity j
            SET j.status = :failed, j.finishedAt = :now, j.nextRetryAt = NULL,
                j.lastErrorType = :errorType, j.lastErrorMessage = :errorMessage,
                j.updatedAt = :now, j.version = j.version + 1
            WHERE j.status = :queued AND j.priority IN :priorities
            """)
    int cancelQueued(
            @Param("priorities") Collection<JobPriority> priorities,
            @Param("queued") JobStatus queued,
            @Param("failed") JobStatus failed,
            @Param("errorType") String errorType,
            @Param("errorMessage") String errorMessage,
            @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE JobEntity j
            SET j.status = :failed, j.finishedAt = :now, j.nextRetryAt = NULL,
                j.lastErrorType = :errorType, j.lastErrorMessage = :errorMessage,
                j.updatedAt = :now, j.version = j.version + 1
            WHERE j.status = :queued AND j.id = :jobId
            """)
    int cancelQueuedById(
            @Param("jobId") String jobId,
            @Param("queued") JobStatus queued,
            @Param("failed") JobStatus failed,
            @Param("errorType") String errorType,
            @Param("errorMessage") String errorMessage,
            @Param("now") Instant now);

    @Query("SELECT j.status, j.priority, COUNT(j) FROM JobEntity j GROUP BY j.status, j.priority")
    List<Object[]> countByStatusAndPriority();

    @Query("""
            SELECT j.priority, COUNT(j) FROM JobEntity j
            WHERE j.status = :queued AND COALESCE(j.nextRetryAt, j.scheduledFor) > :now
            GROUP BY j.priority
            """)
    List<Object[]> countDelayedByPriority(@Param("queued") JobStatus queued, @Param("now") Instant now);

    @Query(value = """
            SELECT * FROM jobs
            WHERE status = 'RUNNING'
              AND lease_expires_at <= :now
            ORDER BY lease_expires_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<JobEntity> lockExpiredRunningJobs(@Param("now") Instant now, @Param("limit") int limit);
}
