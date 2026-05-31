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
package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class JobExecutionEntityTest {

  @Test
  void markSucceededUsesSuppliedEndTime() {
    JobExecutionEntity execution = executionStartedAt("2026-05-09T12:00:00Z");

    execution.markSucceeded(Instant.parse("2026-05-09T12:00:03Z"));

    assertEquals(JobExecutionEntity.ExecutionStatus.SUCCEEDED, execution.getStatus());
    assertEquals(3000L, execution.getDurationMs());
  }

  @Test
  void markFailedUsesSuppliedEndTimeAndCapturesError() {
    JobExecutionEntity execution = executionStartedAt("2026-05-09T12:00:00Z");

    execution.markFailed(
        new IllegalArgumentException("bad input"), Instant.parse("2026-05-09T12:00:02Z"));

    assertEquals(JobExecutionEntity.ExecutionStatus.FAILED, execution.getStatus());
    assertEquals(2000L, execution.getDurationMs());
    assertEquals(IllegalArgumentException.class.getName(), execution.getErrorClass());
    assertEquals("bad input", execution.getErrorMessage());
  }

  @Test
  void markCanceledUsesSuppliedEndTime() {
    JobExecutionEntity execution = executionStartedAt("2026-05-09T12:00:00Z");

    execution.markCanceled(Instant.parse("2026-05-09T12:00:01Z"));

    assertEquals(JobExecutionEntity.ExecutionStatus.CANCELED, execution.getStatus());
    assertEquals(1000L, execution.getDurationMs());
  }

  @Test
  void markFailedRejectsMissingStartTime() {
    JobExecutionEntity execution = new JobExecutionEntity();

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                execution.markFailed(
                    new RuntimeException("boom"), Instant.parse("2026-05-09T12:00:01Z")));

    assertEquals("Execution start time is not set", thrown.getMessage());
  }

  private static JobExecutionEntity executionStartedAt(String startedAt) {
    JobExecutionEntity execution = new JobExecutionEntity();
    execution.setStartedAt(Instant.parse(startedAt));
    return execution;
  }
}
