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
package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.AnnotatedElement;
import org.junit.jupiter.api.Test;
import run.ratchet.api.event.JobPausedEvent;

class ApiIncubatingContractTest {

  @Test
  void jobBuilderFamilyIsIncubating() {
    // The fluent builder family is uniformly incubating at the type level rather than a mix of
    // stable types with incubating methods, so the whole builder surface may evolve together.
    assertIncubating(JobBuilder.class);
    assertIncubating(RecurringJobBuilder.class);
    assertIncubating(BatchBuilder.class);
    assertIncubating(StreamingBatchBuilder.class);
  }

  @Test
  void primaryServicesAndContextAreIncubating() {
    // Both halves of the service surface make the same stability promise, and the execution
    // context that exposes the incubating JobLogger/SignalDecision types is itself incubating.
    assertIncubating(JobSchedulerService.class);
    assertIncubating(JobQueryService.class);
    assertIncubating(JobContext.class);
  }

  @Test
  void jobPausedEventIsReservedIncubatingApi() {
    assertIncubating(JobPausedEvent.class);
  }

  private static void assertIncubating(AnnotatedElement element) {
    assertTrue(element.isAnnotationPresent(Incubating.class), () -> element + " lacks @Incubating");
  }
}
