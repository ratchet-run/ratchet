package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.ri.util.LambdaSerializer;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WorkflowConditionEvaluatorTest {

  private final ClassPolicy classPolicy = className -> true;
  @Mock private BatchStore batchStore;
  @Mock private LambdaSerializer lambdaSerializer;
  private WorkflowConditionEvaluator evaluator;

  private static JobEntity parentJob(JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(1L);
    job.setStatus(status);
    job.setJobType(JobExecutionType.SINGLE);
    return job;
  }

  private static JobEntity batchParent(JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(10L);
    job.setStatus(status);
    job.setJobType(JobExecutionType.BATCH_PARENT);
    return job;
  }

  private static WorkflowConditionEntity condition(WorkflowCondition.ConditionType type) {
    WorkflowConditionEntity c = new WorkflowConditionEntity();
    c.setId(100L);
    c.setConditionType(type);
    return c;
  }

  private static WorkflowConditionEntity conditionWithExpression(
      WorkflowCondition.ConditionType type, String expression) {
    WorkflowConditionEntity c = condition(type);
    c.setConditionExpression(expression);
    return c;
  }

  private static BatchEntity batch(int total, int completed, int failed) {
    BatchEntity b = new BatchEntity();
    b.setId(10L);
    b.setTotalItems(total);
    b.setCompletedItems(completed);
    b.setFailedItems(failed);
    return b;
  }

  @BeforeEach
  void setUp() {
    evaluator = new WorkflowConditionEvaluator(batchStore, lambdaSerializer, classPolicy);
  }

  @Test
  void success_succeededParent_returnsTrue() {
    assertTrue(
        evaluator.evaluate(
            condition(WorkflowCondition.ConditionType.SUCCESS), parentJob(JobStatus.SUCCEEDED)));
  }

  @Test
  void success_failedParent_returnsFalse() {
    assertFalse(
        evaluator.evaluate(
            condition(WorkflowCondition.ConditionType.SUCCESS), parentJob(JobStatus.FAILED)));
  }

  @Test
  void failure_failedParent_returnsTrue() {
    assertTrue(
        evaluator.evaluate(
            condition(WorkflowCondition.ConditionType.FAILURE), parentJob(JobStatus.FAILED)));
  }

  @Test
  void failure_succeededParent_returnsFalse() {
    assertFalse(
        evaluator.evaluate(
            condition(WorkflowCondition.ConditionType.FAILURE), parentJob(JobStatus.SUCCEEDED)));
  }

  @Test
  void batchSuccess_allComplete_returnsTrue() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 10, 0)));

    assertTrue(
        evaluator.evaluate(condition(WorkflowCondition.ConditionType.BATCH_SUCCESS), parent));
  }

  @Test
  void batchSuccess_withFailures_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 8, 2)));

    assertFalse(
        evaluator.evaluate(condition(WorkflowCondition.ConditionType.BATCH_SUCCESS), parent));
  }

  @Test
  void batchSuccess_notBatchParent_returnsFalse() {
    // Standard job type — getBatchForParent returns empty
    assertFalse(
        evaluator.evaluate(
            condition(WorkflowCondition.ConditionType.BATCH_SUCCESS),
            parentJob(JobStatus.SUCCEEDED)));
  }

  @Test
  void batchFailure_withFailures_returnsTrue() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 7, 3)));

    assertTrue(
        evaluator.evaluate(condition(WorkflowCondition.ConditionType.BATCH_FAILURE), parent));
  }

  @Test
  void batchFailure_noFailures_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 10, 0)));

    assertFalse(
        evaluator.evaluate(condition(WorkflowCondition.ConditionType.BATCH_FAILURE), parent));
  }

  @Test
  void batchSuccessRate_aboveThreshold_returnsTrue() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(100, 96, 4)));

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_SUCCESS_RATE, "0.90"),
            parent));
  }

  @Test
  void batchSuccessRate_belowThreshold_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(100, 80, 20)));

    assertFalse(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_SUCCESS_RATE, "0.95"),
            parent));
  }

  @Test
  void batchFailureCount_withinLimit_returnsTrue() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(100, 97, 3)));

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_FAILURE_COUNT, "5"),
            parent));
  }

  @Test
  void batchFailureCount_exceedingLimit_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(100, 90, 10)));

    assertFalse(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_FAILURE_COUNT, "5"),
            parent));
  }

  @Test
  void customCondition_deserializedPredicate_returnsPredicateResult() {
    JobEntity parent = parentJob(JobStatus.SUCCEEDED);
    SerializablePredicate<JobResult<?>> predicate = JobResult::isSuccess;
    when(lambdaSerializer.deserializeJobResultPredicate("serialized-predicate"))
        .thenReturn(predicate);

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.CUSTOM, "serialized-predicate"),
            parent));
  }

  @Test
  void customCondition_deserializationFailure_returnsFalseWithoutHeuristicFallback() {
    JobEntity parent = parentJob(JobStatus.SUCCEEDED);
    parent.setExecutionDurationMs(10L);
    when(lambdaSerializer.deserializeJobResultPredicate("executionTime > 1")).thenReturn(null);

    assertFalse(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.CUSTOM, "executionTime > 1"),
            parent));
  }

  @Test
  void resultValue_deserializedFunction_returnsFunctionResult() {
    JobEntity parent = parentJob(JobStatus.SUCCEEDED);
    parent.setJobResult("150");
    SerializableFunction<Object, Boolean> function = value -> ((Number) value).intValue() > 100;
    when(lambdaSerializer.deserializeResultFunction("serialized-function")).thenReturn(function);

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(
                WorkflowCondition.ConditionType.RESULT_VALUE, "serialized-function"),
            parent));
  }

  @Test
  void batchCustom_deserializedPredicate_returnsPredicateResult() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 9, 1)));
    SerializablePredicate<BatchContext> predicate = context -> context.failedItems() == 1;
    when(lambdaSerializer.deserializeBatchContextPredicate("serialized-batch-predicate"))
        .thenReturn(predicate);

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(
                WorkflowCondition.ConditionType.BATCH_CUSTOM, "serialized-batch-predicate"),
            parent));
  }

  @Test
  void resultValue_classPolicyDenied_returnsFalse() {
    ClassPolicy denyAll = className -> false;
    WorkflowConditionEvaluator restrictedEvaluator =
        new WorkflowConditionEvaluator(batchStore, lambdaSerializer, denyAll);

    JobEntity parent = parentJob(JobStatus.SUCCEEDED);
    parent.setJobResult("150");
    parent.setResultType("java.lang.Integer");

    assertFalse(
        restrictedEvaluator.evaluate(
            conditionWithExpression(
                WorkflowCondition.ConditionType.RESULT_VALUE, "serialized-function"),
            parent));
  }

  @Test
  void batchSuccessRate_zeroTotalItems_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(0, 0, 0)));

    assertFalse(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_SUCCESS_RATE, "0.90"),
            parent));
  }

  @Test
  void evaluate_exceptionThrown_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(anyLong())).thenThrow(new RuntimeException("DB error"));

    assertFalse(
        evaluator.evaluate(condition(WorkflowCondition.ConditionType.BATCH_SUCCESS), parent));
  }
}
