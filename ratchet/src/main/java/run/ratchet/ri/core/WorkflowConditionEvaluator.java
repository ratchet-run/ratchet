package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.security.MethodLookup;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;

/**
 * Evaluates workflow conditions against parent job results to decide which child jobs to schedule.
 */
@ApplicationScoped
public class WorkflowConditionEvaluator {

  private static final Logger log = Logger.getLogger(WorkflowConditionEvaluator.class);

  private final BatchStore batchStore;
  private final BeanResolver beanResolver;
  private final ClassPolicy classPolicy;
  private final PayloadSerializer payloadSerializer;

  protected WorkflowConditionEvaluator() {
    this.batchStore = null;
    this.beanResolver = null;
    this.classPolicy = null;
    this.payloadSerializer = null;
  }

  @Inject
  public WorkflowConditionEvaluator(
      BatchStore batchStore,
      BeanResolver beanResolver,
      ClassPolicy classPolicy,
      PayloadSerializer payloadSerializer) {
    this.batchStore = batchStore;
    this.beanResolver = beanResolver;
    this.classPolicy = classPolicy;
    this.payloadSerializer = payloadSerializer;
  }

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
    } catch (WorkflowConditionConfigurationException e) {
      throw e;
    } catch (Exception e) {
      log.errorf(
          e,
          "Condition evaluation error for %s on job %s: %s",
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
        .map(
            batch -> {
              BatchContext context =
                  new BatchContext(
                      batch.getId(),
                      batch.getTotalItems(),
                      batch.getCompletedItems(),
                      batch.getFailedItems());
              return invokePredicatePayload(condition.getConditionExpression(), context);
            })
        .orElse(false);
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
    return invokePredicatePayload(condition.getConditionExpression(), createJobResult(parentJob));
  }

  private boolean evaluateFailure(JobEntity parentJob) {
    return parentJob.getStatus() == JobStatus.FAILED;
  }

  private boolean evaluateResultCondition(WorkflowConditionEntity condition, JobEntity parentJob) {
    if (parentJob.getJobResult() == null) {
      return false;
    }
    Object jobResult = parseJobResult(parentJob.getJobResult(), parentJob.getResultType());
    if (jobResult == null) {
      return false;
    }
    return invokePredicatePayload(condition.getConditionExpression(), jobResult);
  }

  private boolean evaluateSuccess(JobEntity parentJob) {
    return parentJob.getStatus() == JobStatus.SUCCEEDED;
  }

  /**
   * Deserializes a {@link JobPayload} from {@code expression} and reflectively invokes the
   * described method, passing {@code contextArg} as the runtime argument. Four invocation shapes
   * are supported:
   *
   * <ul>
   *   <li><b>Static method reference</b> ({@code args} empty, {@code parameterTypes} non-empty) —
   *       the SAM parameter maps to the method's first parameter; {@code contextArg} is passed
   *       directly.
   *   <li><b>Static inline lambda</b> ({@code args} non-empty) — null slots in stored args are
   *       filled with {@code contextArg}.
   *   <li><b>Instance method on context</b> ({@code args} empty, {@code !isStatic}) — {@code
   *       contextArg} is the receiver (e.g. {@code JobResult::isSuccess}).
   *   <li><b>Instance method via CDI bean</b> ({@code args} non-empty, {@code !isStatic}) — CDI
   *       bean is the receiver; null slots are filled with {@code contextArg}.
   * </ul>
   */
  private boolean invokePredicatePayload(String expression, Object contextArg) {
    if (expression == null) {
      return false;
    }
    try {
      JobPayload payload = payloadSerializer.deserialize(expression, JobPayload.class);
      if (payload == null) {
        return false;
      }
      Class<?> cls =
          Class.forName(payload.target(), false, Thread.currentThread().getContextClassLoader());
      Method method = findMethod(cls, payload);
      Object target;
      Object[] args;
      if (payload.isStatic()) {
        target = null;
        if (payload.args().isEmpty()) {
          // Static method reference: SAM parameter maps to the method's first parameter
          args = payload.parameterTypes().length > 0 ? new Object[] {contextArg} : new Object[0];
        } else {
          args = fillArgs(payload.args(), contextArg);
        }
      } else if (payload.args().isEmpty()) {
        // Instance method reference where the SAM parameter is the receiver (e.g. Result::isOk)
        target = contextArg;
        args = new Object[0];
      } else {
        target = beanResolver.resolve(cls);
        args = fillArgs(payload.args(), contextArg);
      }
      Object result = method.invoke(target, args);
      return Boolean.TRUE.equals(result);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
      throw new WorkflowConditionConfigurationException(
          "Invalid workflow condition expression metadata: " + e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      throw new WorkflowConditionConfigurationException(
          "Workflow condition expression cannot be invoked with the stored metadata: "
              + e.getMessage(),
          e);
    } catch (RuntimeException e) {
      throw new WorkflowConditionConfigurationException(
          "Workflow condition expression metadata could not be loaded: " + e.getMessage(), e);
    } catch (Exception e) {
      log.errorf(e, "Condition expression evaluation failed: %s", e.getMessage());
      return false;
    }
  }

  private static Object[] fillArgs(List<Object> stored, Object contextArg) {
    Object[] result = new Object[stored.size()];
    for (int i = 0; i < stored.size(); i++) {
      result[i] = stored.get(i) != null ? stored.get(i) : contextArg;
    }
    return result;
  }

  private static Method findMethod(Class<?> cls, JobPayload payload) throws NoSuchMethodException {
    Method method = MethodLookup.findMethod(cls, payload);
    if (method != null) {
      return method;
    }
    throw new NoSuchMethodException(payload.method() + " not found in " + cls.getName());
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
        return payloadSerializer.deserialize(jobResultJson, clazz);
      } else {
        return payloadSerializer.deserialize(jobResultJson, Object.class);
      }
    } catch (SecurityException e) {
      throw e;
    } catch (Exception e) {
      log.warnf("Job result JSON parse error: %s", e.getMessage());
      return jobResultJson;
    }
  }

  private static final class WorkflowConditionConfigurationException extends IllegalStateException {
    private WorkflowConditionConfigurationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
