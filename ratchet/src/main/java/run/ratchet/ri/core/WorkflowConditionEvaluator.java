/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.api.exception.KeyProviderUnavailableException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.UnsupportedEnvelopeVersionException;
import run.ratchet.ri.payload.ArgumentCoercion;
import run.ratchet.ri.security.MethodLookup;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

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
      Instance<BatchStore> batchStore,
      BeanResolver beanResolver,
      ClassPolicy classPolicy,
      PayloadSerializer payloadSerializer) {
    this(
        batchStore.isResolvable() ? batchStore.get() : null,
        beanResolver,
        classPolicy,
        payloadSerializer);
  }

  /** Constructor for tests that supply a store directly (or {@code null} for no batch support). */
  WorkflowConditionEvaluator(
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
    } catch (KeyProviderUnavailableException | UnsupportedEnvelopeVersionException e) {
      // Deferrable: a transient key-provider outage, or a predicate written by a newer Ratchet that
      // this node cannot read yet. Either could decrypt later (once the provider recovers or this
      // node is upgraded), so propagate it and let the scheduler preserve the branches and defer,
      // not collapse it into a false branch decision.
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
    if (batchStore == null || parentJob.getJobType() != JobExecutionType.BATCH_PARENT) {
      return Optional.empty();
    }
    return batchStore.findBatchById(parentJob.getId());
  }

  private JobResult<?> createJobResult(JobEntity job) {
    return JobResult.of(
        job.getStatus() == JobStatus.SUCCEEDED,
        parseJobResult(job.getJobResult(), job.getResultType(), job.getId()),
        job.getLastError(),
        null,
        job.getExecutionDurationMs(),
        job.getExecutionStartTime(),
        job.getExecutionEndTime(),
        createMetadata(job));
  }

  private Map<String, Object> createMetadata(JobEntity job) {
    Map<String, Object> metadata = new HashMap<>();
    putMetadata(metadata, "jobId", job.getId());
    putMetadata(metadata, "jobType", job.getPublicJobType().name());
    putMetadata(metadata, "businessKey", job.getBusinessKey());
    metadata.put("attempts", job.getAttempts());
    return metadata;
  }

  private static void putMetadata(Map<String, Object> metadata, String key, Object value) {
    if (value != null) {
      metadata.put(key, value);
    }
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
              return invokePredicatePayload(
                  condition.getConditionExpression(), context, parentJob.getId());
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
    return invokePredicatePayload(
        condition.getConditionExpression(), createJobResult(parentJob), parentJob.getId());
  }

  private boolean evaluateFailure(JobEntity parentJob) {
    return parentJob.getStatus() == JobStatus.FAILED;
  }

  private boolean evaluateResultCondition(WorkflowConditionEntity condition, JobEntity parentJob) {
    if (parentJob.getJobResult() == null) {
      return false;
    }
    Object jobResult =
        parseJobResult(parentJob.getJobResult(), parentJob.getResultType(), parentJob.getId());
    if (jobResult == null) {
      return false;
    }
    return invokePredicatePayload(condition.getConditionExpression(), jobResult, parentJob.getId());
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
  private boolean invokePredicatePayload(String expression, Object contextArg, UUID parentJobId) {
    if (expression == null) {
      return false;
    }
    try {
      JobPayload payload =
          payloadSerializer.deserialize(
              PayloadEncryptor.decryptArgs(expression, EncryptionTarget.predicate(parentJobId)),
              JobPayload.class);
      if (payload == null) {
        return false;
      }
      verifyClassAllowed(payload.target(), "condition expression target");
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
      Object result =
          method.invoke(target, ArgumentCoercion.coerce(method.getParameterTypes(), args));
      return Boolean.TRUE.equals(result);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      log.errorf(cause, "Condition expression evaluation failed: %s", cause.getMessage());
      return false;
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
      throw new WorkflowConditionConfigurationException(
          "Invalid workflow condition expression metadata: " + e.getMessage(), e);
    } catch (IllegalArgumentException e) {
      throw new WorkflowConditionConfigurationException(
          "Workflow condition expression cannot be invoked with the stored metadata: "
              + e.getMessage(),
          e);
    } catch (SecurityException e) {
      throw e;
    } catch (KeyProviderUnavailableException | UnsupportedEnvelopeVersionException e) {
      // Deferrable: a transient key-provider outage, or a predicate written by a newer Ratchet this
      // node cannot read yet. Stay propagatable -- do not bury it as a permanent configuration
      // error; it resolves once the provider recovers or this node is upgraded.
      throw e;
    } catch (PayloadDecryptionException | KeyNotFoundException e) {
      // Poison: corrupt/tampered ciphertext or a permanently-forgotten key. The predicate can never
      // be evaluated, so it is a permanent configuration failure.
      throw new WorkflowConditionConfigurationException(
          "Workflow condition predicate could not be decrypted: " + e.getMessage(), e);
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

  private void verifyClassAllowed(String className, String usage) {
    if (classPolicy != null && !classPolicy.isAllowed(className)) {
      throw new SecurityException("Class " + className + " is not allowed for " + usage + ".");
    }
  }

  private static Method findMethod(Class<?> cls, JobPayload payload) throws NoSuchMethodException {
    Method method = MethodLookup.findMethod(cls, payload);
    if (method != null) {
      return method;
    }
    throw new NoSuchMethodException(payload.method() + " not found in " + cls.getName());
  }

  /**
   * Deserializes a persisted job result for workflow-condition evaluation.
   *
   * <p>The stored {@code result_type} class name is attacker-controllable given write access to the
   * result columns, so it is gated by {@link ClassPolicy#isAllowedForResultType(String)} — a
   * control strictly narrower than the invocation allowlist used for predicate targets. A result
   * type that is not explicitly allowed for result deserialization is NOT instantiated; the stored
   * JSON is parsed into its native representation (numbers, strings, maps, lists) instead, which is
   * still enough for value comparisons while denying class instantiation by default.
   */
  private Object parseJobResult(String jobResultJson, String resultType, UUID jobId) {
    if (jobResultJson == null) {
      return null;
    }
    // Decrypt at rest before deserializing; marker-driven, so plaintext passes through.
    jobResultJson =
        PayloadEncryptor.decryptJsonColumn(
            jobResultJson, EncryptionTarget.rowBound(ProtectedSurface.RESULT, jobId));
    try {
      if (resultType != null
          && classPolicy != null
          && classPolicy.isAllowedForResultType(resultType)) {
        Class<?> clazz =
            Class.forName(resultType, false, Thread.currentThread().getContextClassLoader());
        return payloadSerializer.deserialize(jobResultJson, clazz);
      }
      return payloadSerializer.deserialize(jobResultJson, Object.class);
    } catch (Exception e) {
      log.warnf(e, "Job result JSON parse error: %s", e.getMessage());
      return jobResultJson;
    }
  }

  private static final class WorkflowConditionConfigurationException extends IllegalStateException {
    private WorkflowConditionConfigurationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
