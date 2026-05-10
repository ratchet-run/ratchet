package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DefaultJobBuilderTest {

  @Test
  void whenResultStoresExplicitPriority() {
    DefaultJobBuilder builder =
        (DefaultJobBuilder) DefaultJobBuilder.create(ignored -> null, () -> {}, Duration.ZERO);

    builder.whenResult(value -> true, () -> {}, 7);

    assertEquals(7, builder.workflowBranches().get(0).condition().priority());
  }
}
