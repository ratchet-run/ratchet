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
package run.ratchet.store.dto;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionType;

class JobCompletionPlanTest {
  @Test
  void batchRequiresCompleteNonnegativeCountersWithoutOverflow() {
    assertThrows(
        IllegalArgumentException.class, () -> new JobCompletionPlan.BatchCompletion(2, 1, 0));
    assertThrows(
        IllegalArgumentException.class, () -> new JobCompletionPlan.BatchCompletion(-1, 0, 0));
    assertThrows(
        IllegalArgumentException.class, () -> new JobCompletionPlan.BatchCompletion(1, -1, 2));
    assertThrows(
        IllegalArgumentException.class, () -> new JobCompletionPlan.BatchCompletion(1, 2, -1));
    assertDoesNotThrow(
        () -> new JobCompletionPlan.BatchCompletion(Integer.MAX_VALUE, Integer.MAX_VALUE, 1));
  }

  @Test
  void dependencyRejectsMissingSnapshotIdentityOrUnsupportedMutation() {
    UUID id = UUID.randomUUID();
    assertThrows(
        NullPointerException.class,
        () -> transition(null, JobStatus.PENDING, JobStatus.PENDING, JobExecutionType.SINGLE));
    assertThrows(
        NullPointerException.class,
        () -> transition(id, null, JobStatus.PENDING, JobExecutionType.SINGLE));
    assertThrows(
        NullPointerException.class,
        () -> transition(id, JobStatus.PENDING, null, JobExecutionType.SINGLE));
    assertThrows(
        NullPointerException.class,
        () -> transition(id, JobStatus.PENDING, JobStatus.PENDING, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> transition(id, JobStatus.PENDING, JobStatus.SUCCEEDED, JobExecutionType.SINGLE));
    assertThrows(
        IllegalArgumentException.class,
        () -> transition(id, JobStatus.PENDING, JobStatus.RUNNING, JobExecutionType.SINGLE));
  }

  private JobCompletionPlan.DependencyTransition transition(
      UUID id, JobStatus expected, JobStatus target, JobExecutionType type) {
    return new JobCompletionPlan.DependencyTransition(id, expected, null, null, target, null, type);
  }
}
