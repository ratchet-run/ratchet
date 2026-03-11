package run.ratchet.api;

/**
 * Categorizes jobs by their execution pattern and orchestration semantics.
 *
 * <p>JobType determines how the scheduler handles job lifecycle, dependencies, and execution flow.
 * Each type implements specific patterns for common distributed computing scenarios, from simple
 * one-off tasks to complex workflow orchestration.
 *
 * <h2>Type Categories:</h2>
 *
 * <ul>
 *   <li><b>Basic Execution:</b> SINGLE, RECURRING - Standard job patterns
 *   <li><b>Batch Processing:</b> BATCH_PARENT, BATCH_CHILD - Parallel work distribution
 *   <li><b>Orchestration:</b> CHAIN_STEP, WORKFLOW_BRANCH, WORKFLOW_JOIN - Complex dependencies
 *   <li><b>System:</b> DLQ_ALERT - Internal scheduler operations
 * </ul>
 *
 * <h2>Scheduler Behaviors by Type:</h2>
 *
 * <ul>
 *   <li>Polling queries use job_type indexes for efficient type-specific operations
 *   <li>Dependency resolution logic varies based on type semantics
 *   <li>Archival policies may differ (e.g., batch children archived with parent)
 *   <li>Monitoring and metrics aggregation considers type hierarchy
 * </ul>
 *
 * @see JobEntity#getJobType()
 * @see JobBuilder
 */
public enum JobType {
  /**
   * Standard one-time execution job scheduled for a specific time.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Executes exactly once at the scheduled time
   *   <li>No automatic rescheduling after completion
   *   <li>Most common job type for ad-hoc tasks
   *   <li>Supports all standard features: retries, timeouts, dependencies
   *   <li>Created via {@code JobScheduler.schedule()} methods
   * </ul>
   */
  SINGLE,

  /**
   * Automatically rescheduled job based on cron expression or fixed intervals.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Automatically creates next instance after completion
   *   <li>Uses cron expressions (e.g., "0 0 * * *" for daily)
   *   <li>Next execution time calculated from configured timezone
   *   <li>Continues until explicitly stopped or end date reached
   *   <li>Created via {@code RecurringScheduler} or cron methods
   *   <li>Indexed separately for efficient recurring job queries
   * </ul>
   */
  RECURRING,

  /**
   * Orchestrator job managing a collection of parallel child jobs.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Tracks overall batch progress via BatchEntity
   *   <li>Completes when all children finish (success or failure)
   *   <li>Provides aggregated metrics across all children
   *   <li>Supports continuation jobs after batch completion
   *   <li>Created via {@code BatchJobBuilder} API
   *   <li>Used for map-reduce patterns and bulk operations
   * </ul>
   */
  BATCH_PARENT,

  /**
   * Individual work unit within a batch processing operation.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Executes independently but reports to parent batch
   *   <li>Updates parent's progress atomically on completion
   *   <li>Failures don't prevent other children from executing
   *   <li>Inherits retry and timeout config from parent
   *   <li>Automatically created by batch job expansion
   *   <li>Archived together with parent for consistency
   * </ul>
   */
  BATCH_CHILD,

  /**
   * Sequential step in a job chain or pipeline.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Executes after previous step completes successfully
   *   <li>Can access results from previous steps in chain
   *   <li>Chain breaks on first failure unless configured otherwise
   *   <li>Created via {@code JobBuilder.then()} fluent API
   *   <li>Supports branching and conditional execution
   *   <li>Used for multi-stage processing pipelines
   * </ul>
   */
  CHAIN_STEP,

  /**
   * System-generated alert job for dead letter queue notifications.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Created automatically on permanent job failures
   *   <li>Triggers administrative notifications and alerts
   *   <li>Contains failure context and diagnostic information
   *   <li>High priority to ensure timely incident response
   *   <li>Not user-creatable - system use only
   *   <li>May trigger escalation workflows
   * </ul>
   */
  DLQ_ALERT,

  /**
   * Conditional branch in a workflow based on parent job results.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Executes only if parent's result matches conditions
   *   <li>Supports complex predicates on parent output
   *   <li>Multiple branches can execute from same parent
   *   <li>Created via workflow builder conditional methods
   *   <li>Enables if-then-else patterns in job flows
   *   <li>Conditions evaluated by WorkflowConditionRepository
   * </ul>
   */
  WORKFLOW_BRANCH,

  /**
   * Synchronization point waiting for multiple parent jobs.
   *
   * <p>Characteristics:
   *
   * <ul>
   *   <li>Waits for ALL configured parents to complete
   *   <li>Supports both "all-success" and "any-complete" semantics
   *   <li>Aggregates results from all parent jobs
   *   <li>Created via workflow builder join methods
   *   <li>Implements fork-join parallelism patterns
   *   <li>Critical for complex DAG workflows
   * </ul>
   */
  WORKFLOW_JOIN
}
