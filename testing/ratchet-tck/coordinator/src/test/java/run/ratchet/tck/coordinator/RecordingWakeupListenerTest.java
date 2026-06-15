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
package run.ratchet.tck.coordinator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.spi.JobWakeupHint;

class RecordingWakeupListenerTest {

  private static final NodeIdentity SRC = new NodeIdentity("node-a");

  @Test
  void awaitCount_passesOnExactMatch() {
    RecordingWakeupListener listener = new RecordingWakeupListener();
    listener.accept(new JobWakeupHint(JobPriority.NORMAL, SRC, null));
    listener.accept(new JobWakeupHint(JobPriority.HIGH, SRC, null));

    assertDoesNotThrow(() -> listener.awaitCount(2, Duration.ofSeconds(1)));
  }

  @Test
  void awaitCount_failsOnUndershoot() {
    RecordingWakeupListener listener = new RecordingWakeupListener();
    listener.accept(new JobWakeupHint(JobPriority.NORMAL, SRC, null));

    AssertionError err =
        assertThrows(AssertionError.class, () -> listener.awaitCount(3, Duration.ofMillis(50)));
    assertTrue(
        err.getMessage().contains("at least 3"),
        "undershoot message should reflect the at-least wait failure, got: " + err.getMessage());
  }

  @Test
  void awaitCount_failsOnOvershoot() {
    // Reproduces a double-delivery bug: the coordinator emitted 3 events when the test asked for
    // exactly 2. The settle window must catch the trailing duplicate.
    RecordingWakeupListener listener = new RecordingWakeupListener();
    listener.accept(new JobWakeupHint(JobPriority.NORMAL, SRC, null));
    listener.accept(new JobWakeupHint(JobPriority.HIGH, SRC, null));
    listener.accept(new JobWakeupHint(JobPriority.HIGH, SRC, null));

    AssertionError err =
        assertThrows(AssertionError.class, () -> listener.awaitCount(2, Duration.ofSeconds(1)));
    assertTrue(
        err.getMessage().contains("exactly 2"),
        "overshoot message should name the exact target, got: " + err.getMessage());
    assertTrue(
        err.getMessage().contains("observed 3"),
        "overshoot message should report the observed count, got: " + err.getMessage());
  }

  @Test
  void awaitAtLeast_passesOnOvershoot() {
    // awaitAtLeast must NOT enforce exactness — that's awaitCount's job.
    RecordingWakeupListener listener = new RecordingWakeupListener();
    listener.accept(new JobWakeupHint(JobPriority.NORMAL, SRC, null));
    listener.accept(new JobWakeupHint(JobPriority.HIGH, SRC, null));
    listener.accept(new JobWakeupHint(JobPriority.HIGH, SRC, null));

    assertDoesNotThrow(() -> listener.awaitAtLeast(2, Duration.ofSeconds(1)));
  }
}
