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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class JobEntityCallerPrincipalTest {

  @Test
  void firstNonNullSet_isAccepted() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    assertEquals("alice", job.getCallerPrincipal());
  }

  @Test
  void idempotentResetToSameValue_isAllowed() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    job.setCallerPrincipal("alice");
    assertEquals("alice", job.getCallerPrincipal());
  }

  @Test
  void overwriteWithDifferentValue_isSilentNoOp() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    job.setCallerPrincipal("bob");
    assertEquals(
        "alice",
        job.getCallerPrincipal(),
        "Caller principal must be write-once: 'bob' must not overwrite 'alice'");
  }

  @Test
  void clearWithNullAfterSet_isSilentNoOp() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal("alice");
    job.setCallerPrincipal(null);
    assertEquals(
        "alice",
        job.getCallerPrincipal(),
        "Caller principal must be write-once: null must not clear 'alice'");
  }

  @Test
  void nullSetBeforeAnyValue_leavesFieldNullAndAllowsLaterSet() {
    JobEntity job = new JobEntity();
    job.setCallerPrincipal(null);
    assertNull(job.getCallerPrincipal());
    job.setCallerPrincipal("alice");
    assertEquals("alice", job.getCallerPrincipal());
  }
}
