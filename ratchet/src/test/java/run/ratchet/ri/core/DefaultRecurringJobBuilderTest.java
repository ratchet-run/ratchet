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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import run.ratchet.api.ExecutorTargets;
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

  @Test
  void platformAndVirtualUseLastCallWinsSemantics() {
    DefaultRecurringJobBuilder builder = newBuilder();

    builder.virtual().platform();
    assertEquals(ExecutorTargets.PLATFORM, builder.executionTarget());

    builder.virtual();
    assertEquals(ExecutorTargets.VIRTUAL, builder.executionTarget());
  }

  private static DefaultRecurringJobBuilder newBuilder() {
    DefaultRecurringJobBuilder builder =
        new DefaultRecurringJobBuilder("* * * * * ?", ZoneId.of("UTC"), () -> {}, ignored -> null);
    builder.withOptions(JobOptions.defaults());
    return builder;
  }
}
