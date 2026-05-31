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
  void jobBuilderMethodsExposeIncubatingWorkflowTypes() throws NoSuchMethodException {
    assertIncubating(
        JobBuilder.class.getMethod(
            "branch", WorkflowCondition.class, SerializableCheckedRunnable.class, String.class));
    assertIncubating(JobBuilder.class.getMethod("workflowBranches"));
  }

  @Test
  void recurringExecutionTargetMethodsAreIncubating() throws NoSuchMethodException {
    assertIncubating(RecurringJobBuilder.class.getMethod("virtual"));
    assertIncubating(RecurringJobBuilder.class.getMethod("platform"));
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
