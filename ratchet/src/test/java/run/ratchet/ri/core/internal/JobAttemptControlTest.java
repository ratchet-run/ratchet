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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobAttemptControlTest {
  private JobAttemptControl newAttempt() {
    return new JobAttemptControl(
        new UUID(0L, 42L), Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0);
  }

  @Test
  void attemptTokensAreStableAndDistinct() {
    JobAttemptControl first = newAttempt();
    JobAttemptControl second = newAttempt();
    assertNotNull(first.attemptToken());
    assertNotNull(second.attemptToken());
    assertSame(first.attemptToken(), first.attemptToken());
    assertNotSame(first.attemptToken(), second.attemptToken());
  }

  @Test
  void workerCanHandTimeoutBackForWatchdogToClaim() {
    JobAttemptControl attempt = newAttempt();
    assertFalse(attempt.isTimeoutHandedBack());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertTrue(attempt.claimTimeoutForWorker());
    assertFalse(attempt.claimTimeoutForWorker());
    assertTrue(attempt.handBackTimeoutToWatchdog());
    assertTrue(attempt.isTimeoutHandedBack());
    assertFalse(attempt.claimTimeoutForWorker());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertTrue(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.isTimeoutHandedBack());
    assertFalse(attempt.claimTimeoutForWatchdog());
  }

  @Test
  void watchdogCanClaimUnclaimedTimeoutAndExcludeWorker() {
    JobAttemptControl attempt = newAttempt();
    assertTrue(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.claimTimeoutForWorker());
    assertFalse(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertFalse(attempt.isTimeoutHandedBack());
  }

  @Test
  void watchdogPassingWorkerPreventsHandBack() {
    JobAttemptControl attempt = newAttempt();
    assertTrue(attempt.claimTimeoutForWorker());
    assertFalse(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.claimTimeoutForWatchdog());
    assertFalse(attempt.handBackTimeoutToWatchdog());
    assertFalse(attempt.isTimeoutHandedBack());
    assertFalse(attempt.claimTimeoutForWorker());
  }
}
