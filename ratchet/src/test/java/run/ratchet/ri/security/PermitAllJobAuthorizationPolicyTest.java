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
package run.ratchet.ri.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobStatus;

/**
 * The permit-all policy overrides only the mutating checks with empty bodies; checkExecute,
 * checkRead and checkDeliverSignal fall through to the SPI defaults. End-to-end permit/deny is
 * covered by JobAuthorizationPolicyIT/DenyIT and the authorization TCK, so this keeps one smoke
 * check plus the reference-identity guarantee of filterForPrincipal, which the ITs do not assert.
 */
class PermitAllJobAuthorizationPolicyTest {

  private final PermitAllJobAuthorizationPolicy policy = new PermitAllJobAuthorizationPolicy();

  @Test
  void checkCreate_permitsAlways() {
    assertDoesNotThrow(() -> policy.checkCreate(UUID.randomUUID(), "alice"));
  }

  @Test
  void filterForPrincipal_returnsOriginalFilterUnchanged() {
    JobFilter original =
        JobFilter.builder()
            .statuses(JobStatus.PENDING)
            .callerPrincipal("requested-principal")
            .businessKey("billing-42")
            .build();

    JobFilter scoped = policy.filterForPrincipal(original, "current-principal");

    assertSame(original, scoped);
  }
}
