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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.AttemptSupersededException;
import run.ratchet.ri.cdi.internal.JsonbPayloadSerializer;
import run.ratchet.ri.core.JobTransaction;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Result;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.spi.JobStateStore.VersionedState;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.store.util.ExtensionValidation;

class JobCheckpointUnitTest {
  private final UUID id = new UUID(0, 42);
  private final JobStateStore store = mock(JobStateStore.class);
  private final TransactionalCompletionStore transactional =
      mock(TransactionalCompletionStore.class);
  private final AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
  private final PayloadSerializer serializer = mock(PayloadSerializer.class);
  private final JobStateFlusher flusher = mock(JobStateFlusher.class);
  private final JobAttemptControl attempt =
      new JobAttemptControl(id, Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0, 7, "node");
  private final List<String> events = Collections.synchronizedList(new ArrayList<>());
  private final Carrier carrier = new Carrier();
  private final List<Runnable> commits = new ArrayList<>();
  private StateWriteResult outcome = StateWriteResult.UPDATED;
  private JobCheckpointWriter writer;

  @BeforeEach
  void setup() {
    when(serializer.serialize(any(), eq((long) ExtensionValidation.MAX_STATE_BYTES)))
        .thenAnswer(call -> ((Map<?, ?>) call.getArgument(0)).get("value"));
    when(serializer.deserialize(anyString(), eq(Map.class)))
        .thenAnswer(call -> Map.of("value", call.getArgument(0)));
    stored("a", 3);
    when(transactional.writeClaimState(
            eq(id),
            eq(JobCheckpointWriter.NAMESPACE),
            anyString(),
            eq(7L),
            nullable(Integer.class)))
        .thenAnswer(
            call -> {
              events.add("write:" + call.getArgument(2) + ":7:" + call.getArgument(4));
              return outcome;
            });
    writer = writer(store, transactional, null, () -> {});
  }

  private JobCheckpointWriter writer(
      JobStateStore state,
      TransactionalCompletionStore tx,
      CheckpointSeedSource seed,
      Runnable firstUse) {
    return new JobCheckpointWriter(
        id,
        attempt,
        state,
        registrar,
        serializer,
        flusher,
        seed,
        new UnitCarrier(carrier, tx, firstUse));
  }

  private void stored(String value, int version) {
    when(store.getState(id, JobCheckpointWriter.NAMESPACE))
        .thenAnswer(
            call -> {
              events.add("getState");
              assertFalse(JobTransaction.active());
              return Optional.of(new VersionedState(value, version));
            });
  }

  private void pending() {
    when(registrar.registerAfterCommit(any()))
        .thenAnswer(
            call -> {
              commits.add(call.getArgument(0));
              return Result.REGISTERED;
            });
    when(store.initState(eq(id), anyString(), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
  }

  @Test
  void unitCommitsWorkAndCheckpointInOneCarrierCall() throws Exception {
    assertEquals(Optional.of("a"), writer.read());
    when(transactional.writeClaimState(id, JobCheckpointWriter.NAMESPACE, "b", 7L, 3))
        .thenAnswer(
            call -> {
              assertEquals(Optional.of("a"), writer.read());
              events.add("write:b:7:3");
              return StateWriteResult.UPDATED;
            });
    // Stubbing calls the setup answer, so clear its event afterwards.
    events.clear();
    assertEquals(
        "b",
        writer.runInTransaction(
            after -> {
              assertEquals("a", after);
              events.add("unit");
              assertEquals(Optional.of("a"), writer.read());
              return "b";
            }));
    assertEquals(List.of("begin", "unit", "write:b:7:3", "commit"), events);
    assertEquals(Optional.of("b"), writer.read());
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
  }

  @Test
  void unitReceivesLatestCommittedCheckpoint() throws Exception {
    for (String[] pair :
        List.of(new String[] {"a", "b"}, new String[] {"b", "c"}, new String[] {"c", "d"})) {
      writer.runInTransaction(
          after -> {
            assertEquals(pair[0], after);
            return pair[1];
          });
    }
    verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "b", 7L, 3);
    verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "c", 7L, 4);
    verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "d", 7L, 5);
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
  }

  @Test
  void unitStartsFromSeedWhenNoRowExists() throws Exception {
    when(store.getState(id, JobCheckpointWriter.NAMESPACE))
        .thenAnswer(
            call -> {
              events.add("getState");
              assertFalse(JobTransaction.active());
              return Optional.empty();
            });
    CheckpointSeedSource seed = mock(CheckpointSeedSource.class);
    when(seed.seed(id)).thenReturn(Optional.of("seed"));
    writer = writer(store, transactional, seed, () -> {});
    writer.runInTransaction(
        after -> {
          assertEquals("seed", after);
          return "b";
        });
    assertTrue(events.indexOf("getState") < events.indexOf("begin"));
    verify(seed).seed(id);
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
    verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "b", 7L, null);
    writer = writer(store, transactional, null, () -> {});
    writer.runInTransaction(
        after -> {
          assertNull(after);
          return "c";
        });
    verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "c", 7L, null);
  }

  @Test
  void unitFailureRollsBackAndKeepsPreviousCheckpoint() {
    IOException failure = new IOException("unit");
    assertSame(
        failure,
        assertThrows(
            IOException.class,
            () ->
                writer.runInTransaction(
                    after -> {
                      throw failure;
                    })));
    assertEquals("rollback:IOException", events.get(events.size() - 1));
    verifyNoInteractions(transactional);
    assertEquals(Optional.of("a"), writer.read());
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
  }

  @Test
  void nullResultRollsBackUnit() {
    assertThrows(NullPointerException.class, () -> writer.runInTransaction(after -> null));
    assertEquals("rollback:NullPointerException", events.get(events.size() - 1));
    verifyNoInteractions(transactional);
    assertEquals(Optional.of("a"), writer.read());
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
  }

  @Test
  void supersededFenceRollsBackAndThrowsAttemptSuperseded() {
    outcome = StateWriteResult.SUPERSEDED;
    assertThrows(AttemptSupersededException.class, () -> writer.runInTransaction(after -> "b"));
    assertTrue(attempt.isSuperseded());
    assertEquals("rollback:AttemptSupersededException", events.get(events.size() - 1));
    assertEquals(Optional.of("a"), writer.read());
  }

  @Test
  void versionConflictRollsBackWithoutRetry() throws Exception {
    outcome = StateWriteResult.VERSION_CONFLICT;
    assertThrows(IllegalStateException.class, () -> writer.runInTransaction(after -> "b"));
    verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "b", 7L, 3);
    assertEquals("rollback:IllegalStateException", events.get(events.size() - 1));
    outcome = StateWriteResult.UPDATED;
    stored("c", 9);
    writer.runInTransaction(
        after -> {
          assertEquals("c", after);
          return "d";
        });
    verify(store, times(2)).getState(id, JobCheckpointWriter.NAMESPACE);
    verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "d", 7L, 9);
    verifyNoMoreInteractions(transactional);
  }

  @Test
  void unknownCommitIsResolvedBeforeNextUnit() throws Exception {
    for (boolean readFirst : List.of(true, false)) {
      writer = writer(store, transactional, null, () -> {});
      stored("a", 3);
      clearInvocations(store, transactional);
      IOException failure = new IOException("commit acknowledgement");
      carrier.commitFailure = failure;
      assertSame(
          failure, assertThrows(IOException.class, () -> writer.runInTransaction(after -> "b")));
      carrier.commitFailure = null;
      stored("b", 4);
      if (readFirst) assertEquals(Optional.of("b"), writer.read());
      writer.runInTransaction(
          after -> {
            assertEquals("b", after);
            return "c";
          });
      verify(store, times(2)).getState(id, JobCheckpointWriter.NAMESPACE);
      verify(transactional).writeClaimState(id, JobCheckpointWriter.NAMESPACE, "c", 7L, 4);
    }
  }

  @Test
  void concurrentUnitInSameAttemptIsRejected() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    var firstThread = Executors.newSingleThreadExecutor();
    var secondThread = Executors.newSingleThreadExecutor();
    try {
      var first =
          firstThread.submit(
              () ->
                  writer.runInTransaction(
                      after -> {
                        entered.countDown();
                        assertTrue(release.await(5, TimeUnit.SECONDS));
                        return "b";
                      }));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      var rejected =
          secondThread.submit(
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () ->
                          writer.runInTransaction(
                              after -> {
                                fail("second unit ran");
                                return "bad";
                              })));
      rejected.get(5, TimeUnit.SECONDS);
      release.countDown();
      assertEquals("b", first.get(5, TimeUnit.SECONDS));
      assertEquals(
          "c",
          secondThread
              .submit(() -> writer.runInTransaction(after -> "c"))
              .get(5, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      firstThread.shutdownNow();
      secondThread.shutdownNow();
    }
  }

  @Test
  void supersededAttemptDoesNotStartUnit() {
    attempt.markSuperseded();
    assertThrows(
        AttemptSupersededException.class,
        () ->
            writer.runInTransaction(
                after -> {
                  fail("unit ran");
                  return "bad";
                }));
    assertTrue(events.isEmpty());
    verifyNoInteractions(store, transactional);
  }

  @Test
  void attemptRevokedDuringUnitRollsBackBeforeStoreWrite() {
    assertThrows(
        AttemptSupersededException.class,
        () ->
            writer.runInTransaction(
                after -> {
                  attempt.revoke();
                  return "b";
                }));
    assertEquals("rollback:AttemptSupersededException", events.get(events.size() - 1));
    verifyNoInteractions(transactional);
  }

  @Test
  void storeWithoutCapabilityThrowsUnsupportedBeforeUnitRuns() {
    for (JobCheckpointWriter unsupported :
        List.of(
            writer(store, null, null, () -> {}),
            writer(null, transactional, null, () -> {}),
            new JobCheckpointWriter(id, attempt, store, registrar, serializer, flusher))) {
      assertThrows(
          UnsupportedOperationException.class,
          () ->
              unsupported.runInTransaction(
                  after -> {
                    fail("unit ran");
                    return "bad";
                  }));
    }
    assertTrue(events.isEmpty());
    verifyNoInteractions(store, transactional);
  }

  @Test
  void oversizedResultRollsBackUnit() {
    JsonbPayloadSerializer real = new JsonbPayloadSerializer();
    when(store.getState(id, JobCheckpointWriter.NAMESPACE)).thenReturn(Optional.empty());
    writer =
        new JobCheckpointWriter(
            id,
            attempt,
            store,
            registrar,
            real,
            flusher,
            null,
            new UnitCarrier(carrier, transactional, () -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> writer.runInTransaction(after -> "x".repeat(ExtensionValidation.MAX_STATE_BYTES)));
    assertEquals("rollback:PayloadTooLargeException", events.get(events.size() - 1));
    verifyNoInteractions(transactional);
    assertEquals(Optional.empty(), writer.read());
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
  }

  @Test
  void openTransactionIsRejectedBeforeCarrier() {
    carrier.open = true;
    assertThrows(
        IllegalStateException.class,
        () ->
            writer.runInTransaction(
                after -> {
                  fail("unit ran");
                  return "bad";
                }));
    assertTrue(events.isEmpty());
    verifyNoInteractions(store, transactional);
  }

  @Test
  void modeSwitchIsAtomicWithPendingFlush() throws Exception {
    when(store.getState(id, JobCheckpointWriter.NAMESPACE)).thenReturn(Optional.empty());
    pending();
    writer.write("x");
    commits.get(0).run();
    assertThrows(
        IllegalStateException.class,
        () ->
            writer.runInTransaction(
                after -> {
                  fail("unit ran");
                  return "bad";
                }));
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(store.initState(id, JobCheckpointWriter.NAMESPACE, "x", 7L))
        .thenAnswer(
            call -> {
              entered.countDown();
              assertTrue(release.await(5, TimeUnit.SECONDS));
              return StateWriteResult.UPDATED;
            });
    var pool = Executors.newFixedThreadPool(2);
    try {
      var flush = pool.submit(() -> writer.flush());
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      var rejected =
          pool.submit(
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () ->
                          writer.runInTransaction(
                              after -> {
                                fail("unit ran");
                                return "bad";
                              })));
      rejected.get(5, TimeUnit.SECONDS);
      assertFalse(flush.isDone());
      verifyNoInteractions(transactional);
      release.countDown();
      flush.get(5, TimeUnit.SECONDS);
      verify(store).initState(id, JobCheckpointWriter.NAMESPACE, "x", 7L);
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void checkpointAndUnitInOneAttemptAreRejected() throws Exception {
    writer.runInTransaction(after -> "b");
    assertThrows(IllegalStateException.class, () -> writer.write("y"));
    verifyNoInteractions(registrar);
    writer = writer(store, transactional, null, () -> {});
    pending();
    writer.write("y");
    assertThrows(IllegalStateException.class, () -> writer.runInTransaction(after -> "b"));
    writer = writer(store, transactional, null, () -> {});
    assertThrows(
        IllegalStateException.class,
        () ->
            writer.runInTransaction(
                after -> {
                  writer.write("y");
                  return "b";
                }));
    assertEquals("rollback:IllegalStateException", events.get(events.size() - 1));
    verify(registrar).registerAfterCommit(any());
  }

  @Test
  void firstUseRunsOnceBeforeTheFirstCarrier() throws Exception {
    writer = writer(store, transactional, null, () -> events.add("firstUse"));
    writer.runInTransaction(after -> "b");
    writer.runInTransaction(after -> "c");
    assertEquals(1, Collections.frequency(events, "firstUse"));
    assertTrue(events.indexOf("firstUse") < events.indexOf("begin"));
  }

  private final class Carrier extends JobTransaction {
    private boolean open;
    private IOException commitFailure;

    @Override
    public boolean hasActiveTransaction() {
      return open;
    }

    @Override
    public <T> T run(Callable<T> work) throws Exception {
      events.add("begin");
      T result;
      try {
        result = insideCarrier(work);
      } catch (Exception failure) {
        events.add("rollback:" + failure.getClass().getSimpleName());
        throw failure;
      }
      if (commitFailure != null) throw commitFailure;
      events.add("commit");
      return result;
    }
  }
}
