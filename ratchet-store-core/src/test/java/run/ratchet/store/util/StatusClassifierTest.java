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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

class StatusClassifierTest {

  @Test
  void recStatusForLiveStatusEncodesOnlyRecurringColdStatuses() {
    assertEquals("P", StatusClassifier.recStatusForLiveStatus(JobStatus.PENDING));
    assertEquals("A", StatusClassifier.recStatusForLiveStatus(JobStatus.PAUSED));
    assertNull(StatusClassifier.recStatusForLiveStatus(JobStatus.RUNNING));
    assertNull(StatusClassifier.recStatusForLiveStatus(JobStatus.SUCCEEDED));
  }

  @Test
  void recStatusDecodeRecognizesRecurringColdStatuses() {
    assertEquals(JobStatus.PENDING, StatusClassifier.recStatusDecode("P"));
    assertEquals(JobStatus.PAUSED, StatusClassifier.recStatusDecode("A"));
    assertNull(StatusClassifier.recStatusDecode(null));
    assertNull(StatusClassifier.recStatusDecode("R"));
  }
}
