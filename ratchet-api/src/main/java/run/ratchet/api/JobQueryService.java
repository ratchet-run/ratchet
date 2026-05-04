package run.ratchet.api;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Read-only query API for job scheduler state, intended for dashboards, CLIs, and admin tooling.
 *
 * <p>This interface is separate from {@link JobSchedulerService}, which is write-only. Callers
 * that only need to observe job state should depend only on this interface.
 *
 * <p>Authorization: implementations apply the configured {@code JobAuthorizationPolicy} on
 * single-job lookups. For list queries, the caller's principal is available to filter or redact
 * results, but per-row enforcement is not mandated by this contract.
 */
@Incubating
public interface JobQueryService {

  /**
   * Returns a paginated list of jobs matching the given filter.
   *
   * @param filter filter criteria; use {@link JobFilter#builder()} to construct
   * @param limit maximum number of results to return
   * @param offset zero-based index of the first result
   * @return a page of matching job summaries
   */
  JobPage<JobSummary> findJobs(JobFilter filter, int limit, int offset);

  /**
   * Returns the full detail view for a single job, including execution history and dependants.
   *
   * @param jobId the job id
   * @return the job detail, or empty if the job does not exist or the caller lacks read access
   */
  Optional<JobDetail> getJobDetail(UUID jobId);

  /**
   * Returns all execution attempts recorded for the given job, ordered by attempt number ascending.
   *
   * @param jobId the job id
   * @return execution history; empty list if the job has never been executed or does not exist
   */
  List<ExecutionHistorySummary> getExecutionHistory(UUID jobId);

  /**
   * Returns a point-in-time snapshot of queue health metrics.
   *
   * <p>The snapshot aggregates counts from the backing store in a single read pass. Counts are
   * best-effort and not transactionally consistent across all fields.
   */
  QueueHealthSnapshot getQueueHealth();

  /**
   * Returns all jobs whose {@code dependsOn} field points at the given parent.
   *
   * @param jobId the parent job id
   * @return direct dependants; empty list if none
   */
  List<JobSummary> getDependants(UUID jobId);

  /**
   * Returns all child jobs for the given batch parent.
   *
   * @param batchParentId the batch parent job id
   * @return batch children; empty list if none or if the job is not a batch parent
   */
  List<JobSummary> getBatchChildren(UUID batchParentId);

  /**
   * Returns all active recurring job master records.
   *
   * @return recurring masters; empty list if none are scheduled
   */
  List<JobSummary> getRecurringMasters();
}
