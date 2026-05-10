package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobOptions;

class DefaultRecurringJobBuilderTest {

  @Test
  void withOptionsRejectsNull() {
    DefaultRecurringJobBuilder builder = newBuilder();

    assertThrows(NullPointerException.class, () -> builder.withOptions(null));
  }

  @Test
  void withTagsDefensivelyCopiesCallerList() {
    DefaultRecurringJobBuilder builder = newBuilder();
    List<String> tags = new ArrayList<>(List.of("alpha"));

    builder.withTags(tags);
    tags.add("beta");

    assertEquals(List.of("alpha"), builder.tags());
    assertThrows(UnsupportedOperationException.class, () -> builder.tags().add("gamma"));
  }

  @Test
  void withTagsTreatsNullAsEmptyReplacement() {
    DefaultRecurringJobBuilder builder = newBuilder();

    builder.withTags(List.of("alpha"));
    builder.withTags(null);

    assertEquals(List.of(), builder.tags());
  }

  private static DefaultRecurringJobBuilder newBuilder() {
    DefaultRecurringJobBuilder builder =
        new DefaultRecurringJobBuilder("* * * * * ?", ZoneId.of("UTC"), () -> {}, ignored -> null);
    builder.withOptions(JobOptions.defaults());
    return builder;
  }
}
