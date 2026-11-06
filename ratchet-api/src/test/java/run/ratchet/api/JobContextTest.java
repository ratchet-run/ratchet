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
import run.ratchet.api.internal.CancellationIdentities;

class JobContextTest {

  @Test
  void plainContextsRejectCheckpointAccess() {
    JobContext context = JobContext.bind(UUID.randomUUID(), null);
    assertThrows(IllegalStateException.class, () -> context.checkpoint("position"));
    assertThrows(IllegalStateException.class, context::lastCheckpoint);
  }

  @Test
  void unitTestAttemptHasThreadSafeInMemoryCheckpoints() {
    UUID id = UUID.randomUUID();
    JobContext context =
        JobContext.bindAttempt(id, null, Map.of(), new AttemptToken(id, 1), () -> true);
    assertEquals(Optional.empty(), context.lastCheckpoint());
    context.checkpoint("page-2");
    assertEquals(Optional.of("page-2"), context.lastCheckpoint());
    JobContext bound = context;
    assertThrows(NullPointerException.class, () -> bound.checkpoint(null));
    context =
        JobContext.bindAttempt(id, null, Map.of(), new AttemptToken(id, 2), () -> true, "page-1");
    assertEquals(Optional.of("page-1"), context.lastCheckpoint());
  }

  @Test
  void shortAndLegacyBindsHaveNoAttempt() {
    UUID id = UUID.randomUUID();
    for (JobContext ctx :
        new JobContext[] {
          JobContext.bind(id, null),
          JobContext.bind(id, null, Map.of()),
          JobContext.bind(id, null, Map.of(), "payload"),
          JobContext.bind(id, null, Map.of(), null, null),
          JobContext.bind(id, null, Map.of(), null, null, null, () -> true, new Object())
        }) {
      assertTrue(ctx.attemptToken().isEmpty());
      assertFalse(ctx.isCurrentAttempt());
    }
    JobContext ctx =
        JobContext.bind(
            id,
            null,
            Map.of(),
            null,
            null,
            null,
            () -> false,
            null,
            null,
            () -> {
              throw new AssertionError("No attempt must not call supplier");
            },
            null);
    assertFalse(ctx.isCurrentAttempt());
  }

  @Test
  void attemptBindsExposeTokenAndConsultSupplier() {
    UUID id = UUID.randomUUID();
    AttemptToken token = new AttemptToken(id, 7);
    AtomicBoolean current = new AtomicBoolean(true);
    JobContext ctx = JobContext.bindAttempt(id, null, Map.of(), token, current::get);
    assertEquals(Optional.of(token), ctx.attemptToken());
    assertTrue(ctx.isCurrentAttempt());
    current.set(false);
    assertFalse(ctx.isCurrentAttempt());
    assertThrows(
        IllegalArgumentException.class,
        () -> JobContext.bindAttempt(UUID.randomUUID(), null, Map.of(), token, current::get));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            JobContext.bind(
                UUID.randomUUID(),
                null,
                Map.of(),
                null,
                null,
                null,
                () -> false,
                null,
                token,
                current::get,
                null));
    assertThrows(
        NullPointerException.class, () -> JobContext.bindAttempt(id, null, Map.of(), token, null));
    ctx =
        JobContext.bind(
            id,
            null,
            Map.of(),
            null,
            null,
            null,
            () -> false,
            new Object(),
            token,
            current::get,
            null);
    assertEquals(Optional.of(token), ctx.attemptToken());
  }

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
        JobContext.bind(
            UUID.randomUUID(), null, Map.of(), null, null, deadline, requested::get, null);
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
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, requested::get, null);
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
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, () -> true, null);
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
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, () -> true, null);
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
        () -> JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, null, null));
  }

  @Test
  void cancellationExceptionMatchesOnlyItsRequestedAttempt() {
    Object tokenA = new Object();
    JobContext contextA =
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, () -> true, tokenA);
    CancellationRequestedException exception =
        CancellationRequestedException.forContext("stop", contextA);
    JobContext.clear();

    assertTrue(CancellationIdentities.isCooperativeStop(exception, tokenA));
    assertFalse(CancellationIdentities.isCooperativeStop(exception, new Object()));
    assertFalse(CancellationIdentities.isCooperativeStop(exception, null));

    JobContext unrequested =
        JobContext.bind(UUID.randomUUID(), null, Map.of(), null, null, null, () -> false, tokenA);
    assertFalse(
        CancellationIdentities.isCooperativeStop(
            CancellationRequestedException.forContext("stop", unrequested), tokenA));
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
