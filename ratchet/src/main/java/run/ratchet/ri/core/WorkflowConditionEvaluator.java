package run.ratchet.ri.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.ri.util.LambdaSerializer;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.util.ObjectMapperFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Evaluates workflow conditions against parent job results to decide which child jobs to schedule.
 */
@ApplicationScoped
public class WorkflowConditionEvaluator {

  private static final Logger log = Logger.getLogger(WorkflowConditionEvaluator.class);

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.get();

  private final BatchStore batchStore;
  private final LambdaSerializer lambdaSerializer;
  private final ClassPolicy classPolicy;

  // Required by CDI proxy
  protected WorkflowConditionEvaluator() {
    this.batchStore = null;
    this.lambdaSerializer = null;
    this.classPolicy = null;
  }

  @Inject
  public WorkflowConditionEvaluator(
      BatchStore batchStore, LambdaSerializer lambdaSerializer, ClassPolicy classPolicy) {
    this.batchStore = batchStore;
    this.lambdaSerializer = lambdaSerializer;
    this.classPolicy = classPolicy;
  }

  /** Returns true if the condition is met and the child job should be scheduled. */
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
      log.errorf(
          e,
          "Failed to evaluate workflow condition %s for job %s: %s",
          condition.getId(),
          parentJob.getId(),
          e.getMessage());
      return false;
    }
  }

  private Optional<BatchEntity> getBatchForParent(JobEntity parentJob) {
    if (parentJob.getJobType() != JobExecutionType.BATCH_PARENT) {
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
    metadata.put("jobType", job.getPublicJobType().name());
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

      SerializablePredicate<BatchContext> predicate =
          lambdaSerializer.deserializeBatchContextPredicate(expression);

      if (predicate != null) {
        return predicate.test(context);
      }

      log.warnf(
          "Simple string-based batch condition expressions are not supported. "
              + "Expression: '%s'. Use SerializablePredicate<BatchContext> instead.",
          expression);
      return false;

    } catch (Exception e) {
      log.error("Failed to evaluate custom batch condition", e);
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
                log.errorf(
                    "Invalid failure count threshold: %s", condition.getConditionExpression());
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
              if (batch.getTotalItems() == 0) {
                return false;
              }
              double actualSuccessRate = batch.getCompletedItems() / (double) batch.getTotalItems();
              try {
                double requiredRate = Double.parseDouble(condition.getConditionExpression());
                return actualSuccessRate >= requiredRate;
              } catch (NumberFormatException e) {
                log.errorf(
                    "Invalid success rate threshold: %s", condition.getConditionExpression());
                return false;
              }
            })
        .orElse(false);
  }

  private boolean evaluateCustomCondition(WorkflowConditionEntity condition, JobEntity parentJob) {
    try {
      JobResult<?> result = createJobResult(parentJob);
      String expression = condition.getConditionExpression();

      if (expression == null) {
        return false;
      }

      SerializablePredicate<JobResult<?>> predicate =
          lambdaSerializer.deserializeJobResultPredicate(expression);

      if (predicate != null) {
        return predicate.test(result);
      }

      log.warn("Failed to deserialize job result predicate, falling back to simple evaluation");

      if (expression.contains("executionTime")) {
        return result.getExecutionTimeMsOrZero() > extractThreshold(expression);
      }

      return result.isSuccess();

    } catch (Exception e) {
      log.error("Failed to evaluate custom condition", e);
      return false;
    }
  }

  private boolean evaluateFailure(JobEntity parentJob) {
    return parentJob.getStatus() == JobStatus.FAILED;
  }

  private boolean evaluateResultCondition(WorkflowConditionEntity condition, JobEntity parentJob) {
    try {
      if (parentJob.getJobResult() == null) {
        return false;
      }

      String expression = condition.getConditionExpression();
      Object jobResult = parseJobResult(parentJob.getJobResult(), parentJob.getResultType());

      SerializableFunction<Object, Boolean> function =
          lambdaSerializer.deserializeResultFunction(expression);

      if (function != null && jobResult != null) {
        return Boolean.TRUE.equals(function.apply(jobResult));
      }

      return false;
    } catch (Exception e) {
      log.error("Failed to evaluate result condition", e);
      return false;
    }
  }

  private boolean evaluateSuccess(JobEntity parentJob) {
    return parentJob.getStatus() == JobStatus.SUCCEEDED;
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
        if (classPolicy != null && !classPolicy.isAllowed(resultType)) {
          throw new SecurityException(
              "Class " + resultType + " is not allowed by ClassPolicy for result deserialization.");
        }
        Class<?> clazz =
            Class.forName(resultType, false, Thread.currentThread().getContextClassLoader());
        return OBJECT_MAPPER.readValue(jobResultJson, clazz);
      } else {
        return OBJECT_MAPPER.readValue(jobResultJson, Object.class);
      }
    } catch (SecurityException e) {
      throw e; // propagate ClassPolicy rejections
    } catch (Exception e) {
      log.warnf("Failed to parse job result: %s", e.getMessage());
      return jobResultJson; // Return as string if parsing fails
    }
  }
}
