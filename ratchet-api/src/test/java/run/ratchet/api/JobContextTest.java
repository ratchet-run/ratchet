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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JobContextTest {

  @AfterEach
  void clearContext() {
    JobContext.clear();
  }

  @Test
  void currentOrNullReturnsNullWhenUnbound() {
    assertNull(JobContext.currentOrNull());
    assertThrows(IllegalStateException.class, JobContext::current);
  }

  @Test
  void bindWithCallerPrincipalExposesCallerPrincipal() {
    UUID jobId = UUID.randomUUID();

    JobContext bound = JobContext.bind(jobId, null, Map.of("tenant", "west"), "alice", null);

    assertSame(bound, JobContext.currentOrNull());
    assertEquals(jobId, JobContext.current().jobId());
    assertEquals("west", JobContext.current().param("tenant"));
    assertEquals("alice", JobContext.current().callerPrincipal());
  }

  @Test
  void existingBindOverloadsExposeNullCallerPrincipal() {
    JobContext.bind(UUID.randomUUID(), null, Map.of());

    assertNull(JobContext.current().callerPrincipal());
  }
}
