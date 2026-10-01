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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.CancellationRequestedException;

class JobContextTest {

  @Test
  void legacyContextHasNoDeadlineOrCancellation() {
    JobContext context = JobContext.bind(UUID.randomUUID(), null);
    assertEquals(Optional.empty(), context.deadline());
    assertFalse(context.isCancellationRequested());
    assertDoesNotThrow(context::throwIfCancellationRequested);
  }

  @Test
  void watchedContextObservesCancellationFromAnotherThread() throws InterruptedException {
    Instant deadline = Instant.parse("2026-10-01T12:00:00Z");
    AtomicBoolean requested = new AtomicBoolean();
    JobContext context =
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, deadline, requested::get);
    assertEquals(Optional.of(deadline), context.deadline());
    assertFalse(context.isCancellationRequested());
    assertDoesNotThrow(context::throwIfCancellationRequested);
    Thread watchdog = new Thread(() -> requested.set(true));
    watchdog.start();
    watchdog.join();
    assertTrue(context.isCancellationRequested());
    assertThrows(CancellationRequestedException.class, context::throwIfCancellationRequested);
  }

  @Test
  void watchedContextRequiresCancellationSupplier() {
    assertThrows(
        NullPointerException.class,
        () -> JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, null));
  }

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
