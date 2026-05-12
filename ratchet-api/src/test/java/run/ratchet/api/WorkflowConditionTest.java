package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class WorkflowConditionTest {

  @Test
  void resultConditionSupportsExplicitPriority() {
    SerializableFunction<String, Boolean> function = value -> true;

    WorkflowCondition condition = WorkflowCondition.result(function, 7);

    assertEquals(WorkflowCondition.ConditionType.RESULT_VALUE, condition.type());
    assertSame(function, condition.expression());
    assertEquals(7, condition.priority());
  }

  @Test
  void workflowBranchRejectsNullConditionAtConstruction() {
    assertThrows(
        NullPointerException.class,
        () -> new WorkflowBranch(null, (SerializableCheckedRunnable) () -> {}));
  }
}
