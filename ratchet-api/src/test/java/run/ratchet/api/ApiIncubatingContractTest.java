package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.AnnotatedElement;
import org.junit.jupiter.api.Test;
import run.ratchet.api.event.JobPausedEvent;

class ApiIncubatingContractTest {

  @Test
  void jobBuilderMethodsExposeIncubatingWorkflowTypes() throws NoSuchMethodException {
    assertIncubating(
        JobBuilder.class.getMethod(
            "branch", WorkflowCondition.class, SerializableCheckedRunnable.class, String.class));
    assertIncubating(JobBuilder.class.getMethod("workflowBranches"));
  }

  @Test
  void streamingBatchBuilderIsIncubating() {
    assertIncubating(StreamingBatchBuilder.class);
  }

  @Test
  void jobPausedEventIsReservedIncubatingApi() {
    assertIncubating(JobPausedEvent.class);
  }

  private static void assertIncubating(AnnotatedElement element) {
    assertTrue(element.isAnnotationPresent(Incubating.class), () -> element + " lacks @Incubating");
  }
}
