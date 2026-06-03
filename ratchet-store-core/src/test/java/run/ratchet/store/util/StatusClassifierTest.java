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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

class StatusClassifierTest {

  @Test
  void effectiveStatusDefaultsNullToPending() {
    assertEquals(JobStatus.PENDING, StatusClassifier.effectiveStatus(null));
    assertEquals(JobStatus.RUNNING, StatusClassifier.effectiveStatus(JobStatus.RUNNING));
  }

  @Test
  void isLiveStatusCoversActiveStatusesIncludingWaiting() {
    assertTrue(StatusClassifier.isLiveStatus(JobStatus.PENDING));
    assertTrue(StatusClassifier.isLiveStatus(JobStatus.RUNNING));
    assertTrue(StatusClassifier.isLiveStatus(JobStatus.PAUSED));
    assertTrue(StatusClassifier.isLiveStatus(JobStatus.WAITING));
    assertFalse(StatusClassifier.isLiveStatus(JobStatus.SUCCEEDED));
    assertFalse(StatusClassifier.isLiveStatus(JobStatus.CANCELED));
  }

  @Test
  void isTerminalStatusCoversTerminalStatusesOnly() {
    assertTrue(StatusClassifier.isTerminalStatus(JobStatus.SUCCEEDED));
    assertTrue(StatusClassifier.isTerminalStatus(JobStatus.FAILED));
    assertTrue(StatusClassifier.isTerminalStatus(JobStatus.CANCELED));
    assertFalse(StatusClassifier.isTerminalStatus(JobStatus.WAITING));
    assertFalse(StatusClassifier.isTerminalStatus(JobStatus.PENDING));
  }
}
