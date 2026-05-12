package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.JobStatus;
import run.ratchet.api.SerializablePredicate;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.ri.cdi.JsonbPayloadSerializer;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;

@ExtendWith(MockitoExtension.class)
class WorkflowConditionEvaluatorTest {

  /** Static predicate methods used as method references in tests — avoids CDI bean lookup. */
  public static final class TestConditions {
    public static boolean batchHasOneFailure(BatchContext ctx) {
      return ctx.failedItems() == 1;
    }

    public static boolean isHighValue(Object value) {
      return ((Number) value).intValue() > 100;
    }

    public static boolean jobSucceeded(JobResult<?> result) {
      return result.isSuccess();
    }
  }

  public static final class BeanCondition {
    public boolean hasExpectedFailureCount(BatchContext ctx) {
      return ctx.failedItems() == 2;
    }
  }

  private final ClassPolicy classPolicy = className -> true;
  private final PayloadSerializer payloadSerializer = new JsonbPayloadSerializer();
  @Mock private BatchStore batchStore;
  @Mock private BeanResolver beanResolver;
  private WorkflowConditionEvaluator evaluator;

  private static JobEntity parentJob(JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(new UUID(0L, 1L));
    job.setStatus(status);
    job.setJobType(JobExecutionType.SINGLE);
    return job;
  }

  private static JobEntity batchParent(JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(new UUID(0L, 10L));
    job.setStatus(status);
    job.setJobType(JobExecutionType.BATCH_PARENT);
    return job;
  }

  private static WorkflowConditionEntity condition(WorkflowCondition.ConditionType type) {
    WorkflowConditionEntity c = new WorkflowConditionEntity();
    c.setId(new UUID(0L, 100L));
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
    b.setId(new UUID(0L, 10L));
    b.setTotalItems(total);
    b.setCompletedItems(completed);
    b.setFailedItems(failed);
    return b;
  }

  private String serializeCondition(Serializable predicate) {
    return payloadSerializer.serialize(JobPayloadFactory.fromLambda(predicate));
  }

  @BeforeEach
  void setUp() {
    evaluator =
        new WorkflowConditionEvaluator(batchStore, beanResolver, classPolicy, payloadSerializer);
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
  void batchSuccessRate_invalidThreshold_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(100, 80, 20)));

    assertFalse(
        evaluator.evaluate(
            conditionWithExpression(
                WorkflowCondition.ConditionType.BATCH_SUCCESS_RATE, "not-a-number"),
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
  void batchFailureCount_invalidThreshold_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(100, 90, 10)));

    assertFalse(
        evaluator.evaluate(
            conditionWithExpression(
                WorkflowCondition.ConditionType.BATCH_FAILURE_COUNT, "not-a-number"),
            parent));
  }

  @Test
  void customCondition_methodRefOnContext_evaluates() {
    // JobResult::isSuccess — instance method called on the JobResult context arg; no CDI bean
    // needed
    JobEntity parent = parentJob(JobStatus.SUCCEEDED);
    String expression =
        serializeCondition((SerializablePredicate<JobResult<?>>) JobResult::isSuccess);

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.CUSTOM, expression), parent));
  }

  @Test
  void customCondition_nullExpression_returnsFalse() {
    assertFalse(
        evaluator.evaluate(
            condition(WorkflowCondition.ConditionType.CUSTOM), parentJob(JobStatus.SUCCEEDED)));
  }

  @Test
  void customCondition_unknownTargetClass_failsHard() {
    String expression =
        payloadSerializer.serialize(
            new JobPayload("com.example.DoesNotExist", "test", "()Z", true, List.of()));

    assertThrows(
        IllegalStateException.class,
        () ->
            evaluator.evaluate(
                conditionWithExpression(WorkflowCondition.ConditionType.CUSTOM, expression),
                parentJob(JobStatus.SUCCEEDED)));
  }

  @Test
  void customCondition_unknownMethod_failsHard() {
    String expression =
        payloadSerializer.serialize(
            new JobPayload(TestConditions.class.getName(), "doesNotExist", "()Z", true, List.of()));

    assertThrows(
        IllegalStateException.class,
        () ->
            evaluator.evaluate(
                conditionWithExpression(WorkflowCondition.ConditionType.CUSTOM, expression),
                parentJob(JobStatus.SUCCEEDED)));
  }

  @Test
  void customCondition_classPolicyDeniedTarget_returnsFalse() {
    ClassPolicy denyConditionTarget =
        className -> !TestConditions.class.getName().equals(className);
    WorkflowConditionEvaluator restrictedEvaluator =
        new WorkflowConditionEvaluator(
            batchStore, beanResolver, denyConditionTarget, payloadSerializer);
    String expression =
        payloadSerializer.serialize(
            new JobPayload(
                TestConditions.class.getName(),
                "jobSucceeded",
                "(Lrun/ratchet/api/JobResult;)Z",
                true,
                List.of()));

    assertFalse(
        restrictedEvaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.CUSTOM, expression),
            parentJob(JobStatus.SUCCEEDED)));
  }

  @Test
  void resultValue_staticMethodRef_evaluates() {
    // TestConditions::isHighValue — static method reference; no CDI bean needed
    JobEntity parent = parentJob(JobStatus.SUCCEEDED);
    parent.setJobResult("150");
    parent.setResultType("java.lang.Integer");
    String expression =
        serializeCondition((SerializablePredicate<Object>) TestConditions::isHighValue);

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.RESULT_VALUE, expression),
            parent));
  }

  @Test
  void resultValue_classPolicyDenied_returnsFalse() {
    ClassPolicy denyAll = className -> false;
    WorkflowConditionEvaluator restrictedEvaluator =
        new WorkflowConditionEvaluator(batchStore, beanResolver, denyAll, payloadSerializer);

    JobEntity parent = parentJob(JobStatus.SUCCEEDED);
    parent.setJobResult("150");
    parent.setResultType("java.lang.Integer");

    // ClassPolicy blocks result-type deserialization before the expression is evaluated
    assertFalse(
        restrictedEvaluator.evaluate(
            condition(WorkflowCondition.ConditionType.RESULT_VALUE), parent));
  }

  @Test
  void batchCustom_staticMethodRef_evaluates() {
    // TestConditions::batchHasOneFailure — static method reference; no CDI bean needed
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 9, 1)));
    String expression =
        serializeCondition(
            (SerializablePredicate<BatchContext>) TestConditions::batchHasOneFailure);

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_CUSTOM, expression),
            parent));
  }

  @Test
  void batchCustom_staticMethodRefFalse_returnsFalse() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 10, 0)));
    String expression =
        serializeCondition(
            (SerializablePredicate<BatchContext>) TestConditions::batchHasOneFailure);

    assertFalse(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_CUSTOM, expression),
            parent));
  }

  @Test
  void batchCustom_cdiBeanReceiver_evaluates() {
    JobEntity parent = batchParent(JobStatus.SUCCEEDED);
    when(batchStore.findBatchById(parent.getId())).thenReturn(Optional.of(batch(10, 8, 2)));
    BeanCondition bean = new BeanCondition();
    when(beanResolver.resolve(BeanCondition.class)).thenReturn(bean);
    String expression =
        payloadSerializer.serialize(
            new JobPayload(
                BeanCondition.class.getName(),
                "hasExpectedFailureCount",
                "(Lrun/ratchet/api/BatchContext;)Z",
                false,
                Collections.singletonList(null)));

    assertTrue(
        evaluator.evaluate(
            conditionWithExpression(WorkflowCondition.ConditionType.BATCH_CUSTOM, expression),
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
    when(batchStore.findBatchById(any(UUID.class))).thenThrow(new RuntimeException("DB error"));

    assertFalse(
        evaluator.evaluate(condition(WorkflowCondition.ConditionType.BATCH_SUCCESS), parent));
  }
}
