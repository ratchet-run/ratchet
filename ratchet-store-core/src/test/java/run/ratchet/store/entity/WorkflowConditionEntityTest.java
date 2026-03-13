package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import run.ratchet.api.BatchContext;
import run.ratchet.api.JobResult;
import run.ratchet.api.SerializableFunction;
import run.ratchet.api.SerializablePredicate;
import org.junit.jupiter.api.Test;

class WorkflowConditionEntityTest {

  @Test
  void setConditionExpressionSerialized_serializesCustomPredicate() {
    WorkflowConditionEntity entity = new WorkflowConditionEntity();
    SerializablePredicate<JobResult<Integer>> predicate =
        result -> result.getValue() != null && result.getValue() > 10;

    entity.setConditionExpressionSerialized(predicate);

    assertNotNull(entity.getConditionExpression());
    assertNotEquals(predicate.toString(), entity.getConditionExpression());
  }

  @Test
  void setConditionExpressionSerialized_serializesResultFunction() {
    WorkflowConditionEntity entity = new WorkflowConditionEntity();
    SerializableFunction<Integer, Boolean> function = value -> value > 10;

    entity.setConditionExpressionSerialized(function);

    assertNotNull(entity.getConditionExpression());
    assertNotEquals(function.toString(), entity.getConditionExpression());
  }

  @Test
  void setConditionExpressionSerialized_preservesLiteralThresholds() {
    WorkflowConditionEntity entity = new WorkflowConditionEntity();

    entity.setConditionExpressionSerialized(5);

    assertEquals("5", entity.getConditionExpression());
  }

  @Test
  void setConditionExpressionSerialized_serializesBatchPredicate() {
    WorkflowConditionEntity entity = new WorkflowConditionEntity();
    SerializablePredicate<BatchContext> predicate = context -> context.failedItems() == 0;

    entity.setConditionExpressionSerialized(predicate);

    assertNotNull(entity.getConditionExpression());
    assertNotEquals(predicate.toString(), entity.getConditionExpression());
  }
}
