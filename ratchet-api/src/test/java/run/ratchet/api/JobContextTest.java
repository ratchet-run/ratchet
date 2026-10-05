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
import java.util.concurrent.atomic.AtomicReference;
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
  void cancellationExceptionRecordsRequestStateWhenCreated() {
    AtomicBoolean requested = new AtomicBoolean();
    JobContext context =
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, requested::get);
    CancellationRequestedException early = new CancellationRequestedException("stop");
    requested.set(true);
    CancellationRequestedException late = new CancellationRequestedException("stop");
    CancellationRequestedException thrown =
        assertThrows(CancellationRequestedException.class, context::throwIfCancellationRequested);

    assertFalse(early.isCancellationRequested());
    assertTrue(late.isCancellationRequested());
    assertTrue(thrown.isCancellationRequested());
  }

  @Test
  void cancellationExceptionWithoutContextRecordsNoRequest() {
    assertFalse(new CancellationRequestedException("stop").isCancellationRequested());
    assertFalse(
        new CancellationRequestedException("stop", new IllegalStateException())
            .isCancellationRequested());
  }

  @Test
  void cancellationExceptionWithNullCauseRecordsNoRequestWithoutContext() {
    CancellationRequestedException exception = new CancellationRequestedException("stop", null);

    assertNull(exception.getCause());
    assertFalse(exception.isCancellationRequested());
  }

  @Test
  void forContextRecordsRequestStateOfGivenContextOffTheJobThread() throws InterruptedException {
    JobContext context =
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, () -> true);
    JobContext.clear();
    IllegalStateException cause = new IllegalStateException();
    AtomicReference<CancellationRequestedException> plain = new AtomicReference<>();
    AtomicReference<CancellationRequestedException> withCause = new AtomicReference<>();
    AtomicReference<CancellationRequestedException> noContext = new AtomicReference<>();
    Thread other =
        new Thread(
            () -> {
              plain.set(CancellationRequestedException.forContext("stop", context));
              withCause.set(CancellationRequestedException.forContext("stop", cause, context));
              noContext.set(CancellationRequestedException.forContext("stop", null));
            });
    other.start();
    other.join();

    assertTrue(plain.get().isCancellationRequested());
    assertTrue(withCause.get().isCancellationRequested());
    assertSame(cause, withCause.get().getCause());
    assertFalse(noContext.get().isCancellationRequested());
  }

  @Test
  void throwIfCancellationRequestedRecordsRequestOffTheJobThread() throws InterruptedException {
    JobContext context =
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, () -> true);
    AtomicReference<CancellationRequestedException> thrown = new AtomicReference<>();
    Thread worker =
        new Thread(
            () -> {
              try {
                context.throwIfCancellationRequested();
              } catch (CancellationRequestedException e) {
                thrown.set(e);
              }
            });
    worker.start();
    worker.join();

    assertTrue(thrown.get().isCancellationRequested());
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
