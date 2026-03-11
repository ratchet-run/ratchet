package run.ratchet.ri.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.JobType;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.spi.SerializationStrategy;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service responsible for evaluating workflow conditions to determine whether child jobs should be
 * scheduled based on parent job results. This evaluator is central to the workflow orchestration
 * system, enabling complex job dependencies and conditional execution paths.
 *
 * <p>The evaluator supports multiple condition types:
 *
 * <ul>
 *   <li><b>Simple Status:</b> SUCCESS, FAILURE - Basic parent job outcome checks
 *   <li><b>Result-based:</b> CUSTOM, RESULT_VALUE - Complex conditions on job output
 *   <li><b>Batch-aware:</b> BATCH_SUCCESS, BATCH_FAILURE_COUNT - Aggregate batch metrics
 * </ul>
 *
 * @see WorkflowConditionEntity for condition storage
 * @see WorkflowScheduler for the scheduling component
 * @see JobResult for the result data structure
 */
@ApplicationScoped
public class WorkflowConditionEvaluator {

  private static final Logger log = Logger.getLogger(WorkflowConditionEvaluator.class.getName());

  /** Jackson ObjectMapper for deserializing job result JSON. */
  private final ObjectMapper objectMapper = new ObjectMapper();

  /** Store for accessing batch job information. */
  private final BatchStore batchStore;

  /** Strategy for serializing/deserializing lambda expressions used in custom conditions. */
  private final SerializationStrategy serializationStrategy;

  // Required by CDI proxy
  protected WorkflowConditionEvaluator() {
    this.batchStore = null;
    this.serializationStrategy = null;
  }

  @Inject
  public WorkflowConditionEvaluator(
      BatchStore batchStore, SerializationStrategy serializationStrategy) {
    this.batchStore = batchStore;
    this.serializationStrategy = serializationStrategy;
  }

  /**
   * Evaluates whether a workflow condition is met based on the parent job's execution result.
   *
   * @param condition the workflow condition to evaluate
   * @param parentJob the parent job that completed
   * @return true if the condition is met and the child job should be scheduled
   */
  public boolean evaluate(WorkflowConditionEntity condition, JobEntity parentJob) {
    try {
      return switch (condition.getConditionType()) {
        case SUCCESS -> evaluateSuccess(parentJob);
        case FAILURE -> evaluateFailure(parentJob);
        case CUSTOM -> evaluateCustomCondition(condition, parentJob);
        case RESULT_VALUE -> evaluateResultCondition(condition, parentJob);
        case BATCH_SUCCESS -> evaluateBatchSuccess(parentJob);
        case BATCH_FAILURE -> evaluateBatchFailure(parentJob);
        case BATCH_SUCCESS_RATE -> evaluateBatchSuccessRate(condition, parentJob);
        case BATCH_FAILURE_COUNT -> evaluateBatchFailureCount(condition, parentJob);
        case BATCH_CUSTOM -> evaluateBatchCustom(condition, parentJob);
      };
    } catch (Exception e) {
      log.log(
          Level.SEVERE,
          "Failed to evaluate workflow condition "
              + condition.getId()
              + " for job "
              + parentJob.getId()
              + ": "
              + e.getMessage(),
          e);
      return false;
    }
  }

  private Optional<BatchEntity> getBatchForParent(JobEntity parentJob) {
    if (parentJob.getJobType() != JobType.BATCH_PARENT) {
      return Optional.empty();
    }
    return batchStore.findBatchById(parentJob.getId());
  }

  private JobResult<?> createJobResult(JobEntity job) {
    return JobResult.of(
        job.getStatus() == JobStatus.SUCCEEDED,
        parseJobResult(job.getJobResult(), job.getResultType()),
        job.getLastError(),
        null,
        job.getExecutionDurationMs(),
        job.getExecutionStartTime(),
        job.getExecutionEndTime(),
        createMetadata(job));
  }

  private Map<String, Object> createMetadata(JobEntity job) {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("jobId", job.getId());
    metadata.put("jobType", job.getJobType().name());
    metadata.put("businessKey", job.getBusinessKey());
    metadata.put("attempts", job.getAttempts());
    return metadata;
  }

  private boolean evaluateBatchCustom(WorkflowConditionEntity condition, JobEntity parentJob) {
    return getBatchForParent(parentJob)
        .map(batch -> evaluateBatchCustomPredicate(condition, batch))
        .orElse(false);
  }

  private boolean evaluateBatchCustomPredicate(
      WorkflowConditionEntity condition, BatchEntity batch) {
    try {
      BatchContext context =
          new BatchContext(
              batch.getId(),
              batch.getTotalItems(),
              batch.getCompletedItems(),
              batch.getFailedItems());

      String expression = condition.getConditionExpression();

      @SuppressWarnings("unchecked")
      SerializablePredicate<BatchContext> predicate =
          (SerializablePredicate<BatchContext>)
              serializationStrategy.deserialize(expression.getBytes(), SerializablePredicate.class);

      if (predicate != null) {
        return predicate.test(context);
      }

      log.warning("Failed to deserialize batch predicate, falling back to simple evaluation");
      return evaluateSimpleBatchCondition(expression, context);

    } catch (Exception e) {
      log.log(Level.SEVERE, "Failed to evaluate custom batch condition", e);
      return false;
    }
  }

  private boolean evaluateBatchFailure(JobEntity parentJob) {
    return getBatchForParent(parentJob).map(batch -> batch.getFailedItems() > 0).orElse(false);
  }

  private boolean evaluateBatchFailureCount(
      WorkflowConditionEntity condition, JobEntity parentJob) {
    return getBatchForParent(parentJob)
        .map(
            batch -> {
              try {
                int maxFailures = Integer.parseInt(condition.getConditionExpression());
                return batch.getFailedItems() <= maxFailures;
              } catch (NumberFormatException e) {
                log.severe(
                    "Invalid failure count threshold: " + condition.getConditionExpression());
                return false;
              }
            })
        .orElse(false);
  }

  private boolean evaluateBatchSuccess(JobEntity parentJob) {
    return getBatchForParent(parentJob)
        .map(
            batch ->
                batch.getFailedItems() == 0 && batch.getCompletedItems() == batch.getTotalItems())
        .orElse(false);
  }

  private boolean evaluateBatchSuccessRate(WorkflowConditionEntity condition, JobEntity parentJob) {
    return getBatchForParent(parentJob)
        .map(
            batch -> {
              double actualSuccessRate = batch.getCompletedItems() / (double) batch.getTotalItems();
              try {
                double requiredRate = Double.parseDouble(condition.getConditionExpression());
                return actualSuccessRate >= requiredRate;
              } catch (NumberFormatException e) {
                log.severe("Invalid success rate threshold: " + condition.getConditionExpression());
                return false;
              }
            })
        .orElse(false);
  }

  @SuppressWarnings("unchecked")
  private boolean evaluateCustomCondition(WorkflowConditionEntity condition, JobEntity parentJob) {
    try {
      JobResult<?> result = createJobResult(parentJob);
      String expression = condition.getConditionExpression();

      if (expression == null) {
        return false;
      }

      // Try to deserialize lambda expression
      SerializablePredicate<JobResult<?>> predicate =
          (SerializablePredicate<JobResult<?>>)
              serializationStrategy.deserialize(expression.getBytes(), SerializablePredicate.class);

      if (predicate != null) {
        return predicate.test(result);
      }

      // Fallback to simple expression evaluation for backward compatibility
      log.warning("Failed to deserialize job result predicate, falling back to simple evaluation");

      if (expression.contains("executionTime")) {
        return result.getExecutionTimeMsOrZero() > extractThreshold(expression);
      }

      return result.isSuccess();

    } catch (Exception e) {
      log.log(Level.SEVERE, "Failed to evaluate custom condition", e);
      return false;
    }
  }

  private boolean evaluateFailure(JobEntity parentJob) {
    return parentJob.getStatus() == JobStatus.FAILED;
  }

  @SuppressWarnings("unchecked")
  private boolean evaluateResultCondition(WorkflowConditionEntity condition, JobEntity parentJob) {
    try {
      if (parentJob.getJobResult() == null) {
        return false;
      }

      String expression = condition.getConditionExpression();
      Object jobResult = parseJobResult(parentJob.getJobResult(), parentJob.getResultType());

      SerializablePredicate<Object> predicate =
          (SerializablePredicate<Object>)
              serializationStrategy.deserialize(expression.getBytes(), SerializablePredicate.class);

      if (predicate != null && jobResult != null) {
        return predicate.test(jobResult);
      }

      if (expression != null && jobResult != null) {
        return evaluateSimpleResultCondition(expression, jobResult);
      }

      return false;
    } catch (Exception e) {
      log.log(Level.SEVERE, "Failed to evaluate result condition", e);
      return false;
    }
  }

  @SuppressWarnings("java:S1172")
  private boolean evaluateSimpleBatchCondition(String expression, BatchContext context) {
    throw new UnsupportedOperationException(
        "Simple string-based batch condition expressions are not supported. Expression: '"
            + expression
            + "'. Use StreamingBatchBuilder.thenWhenBatch() or "
            + "BatchBuilder.thenWhenBatch() with a SerializablePredicate<BatchContext> instead. "
            + "Example: .thenWhenBatch(ctx -> ctx.failedItems() < 5, "
            + "() -> handlePartialSuccess())");
  }

  private boolean evaluateSimpleResultCondition(String expression, Object result) {
    if (result instanceof Number num) {
      if (expression.contains("> 100")) {
        return num.doubleValue() > 100;
      }
      if (expression.contains("< 10")) {
        return num.doubleValue() < 10;
      }
    }

    if (result instanceof String str && expression.contains("contains")) {
      String searchTerm = extractStringFromExpression(expression);
      return str.contains(searchTerm);
    }

    return false;
  }

  private boolean evaluateSuccess(JobEntity parentJob) {
    return parentJob.getStatus() == JobStatus.SUCCEEDED;
  }

  private String extractStringFromExpression(String expression) {
    int start = expression.indexOf('"');
    int end = expression.lastIndexOf('"');
    if (start >= 0 && end > start) {
      return expression.substring(start + 1, end);
    }
    return "";
  }

  private long extractThreshold(String expression) {
    try {
      String[] parts = expression.split(">");
      if (parts.length == 2) {
        return Long.parseLong(parts[1].trim());
      }
    } catch (Exception e) {
      // Ignore parsing errors
    }
    return 0;
  }

  private Object parseJobResult(String jobResultJson, String resultType) {
    if (jobResultJson == null) {
      return null;
    }

    try {
      if (resultType != null) {
        Class<?> clazz = Class.forName(resultType);
        return objectMapper.readValue(jobResultJson, clazz);
      } else {
        return objectMapper.readValue(jobResultJson, Object.class);
      }
    } catch (Exception e) {
      log.warning("Failed to parse job result: " + e.getMessage());
      return jobResultJson; // Return as string if parsing fails
    }
  }
}
