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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.exception.AttemptSupersededException;
import run.ratchet.ri.cdi.internal.JsonbPayloadSerializer;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Result;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.store.spi.JobStateStore.VersionedState;
import run.ratchet.store.util.ExtensionValidation;

class JobCheckpointWriterTest {
  private final UUID id = new UUID(0, 42);
  private final JobStateStore store = mock(JobStateStore.class);
  private final AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
  private final PayloadSerializer serializer = mock(PayloadSerializer.class);
  private final CheckpointFlusher flusher = mock(CheckpointFlusher.class);
  private final JobAttemptControl attempt =
      new JobAttemptControl(id, Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0, 7, "node");
  private final List<Runnable> commits = new ArrayList<>();
  private JobCheckpointWriter writer;

  @BeforeEach
  void setup() {
    writer = new JobCheckpointWriter(id, attempt, store, registrar, serializer, flusher);
    when(serializer.serialize(any(), eq((long) ExtensionValidation.MAX_STATE_BYTES)))
        .thenAnswer(call -> ((Map<?, ?>) call.getArgument(0)).get("value"));
    doAnswer(
            call -> {
              commits.add(call.getArgument(0));
              return Result.NO_ACTIVE_TRANSACTION;
            })
        .when(registrar)
        .registerAfterCommit(any());
    when(store.getState(id, JobCheckpointWriter.NAMESPACE)).thenReturn(Optional.empty());
    when(store.initState(eq(id), anyString(), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    when(store.updateState(eq(id), anyString(), anyString(), anyInt(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
  }

  @Test
  void absentStateIsInitializedAndSteadyStateUsesOneUpdate() {
    writer.write("one");
    verify(store).initState(id, JobCheckpointWriter.NAMESPACE, "one", 7L);
    clearInvocations(store);
    registered();
    writer.write("two");
    commits.get(1).run();
    writer.flush();
    verify(store).updateState(id, JobCheckpointWriter.NAMESPACE, "two", 0, 7L);
    verifyNoMoreInteractions(store);
    assertEquals(Optional.of("two"), writer.read());
  }

  @Test
  void presentStateUsesItsVersion() {
    when(store.getState(id, JobCheckpointWriter.NAMESPACE))
        .thenReturn(Optional.of(new VersionedState("old", 8)));
    writer.write("new");
    verify(store).updateState(id, JobCheckpointWriter.NAMESPACE, "new", 8, 7L);
    verify(store, never()).initState(any(), anyString(), anyString(), anyLong());
  }

  @Test
  void versionConflictRereadsAndRetries() {
    when(store.initState(id, JobCheckpointWriter.NAMESPACE, "new", 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT, StateWriteResult.UPDATED);
    writer.write("new");
    verify(store, times(2)).getState(id, JobCheckpointWriter.NAMESPACE);
  }

  @Test
  void knownVersionConflictFallsBackToOneRead() {
    writer.write("one");
    clearInvocations(store);
    when(store.updateState(id, JobCheckpointWriter.NAMESPACE, "two", 0, 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT, StateWriteResult.UPDATED);
    when(store.getState(id, JobCheckpointWriter.NAMESPACE))
        .thenReturn(Optional.of(new VersionedState("one", 0)));
    writer.write("two");
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
    verify(store, times(2)).updateState(id, JobCheckpointWriter.NAMESPACE, "two", 0, 7L);
  }

  @Test
  void fiveConflictsFail() {
    when(store.initState(id, JobCheckpointWriter.NAMESPACE, "new", 7L))
        .thenReturn(StateWriteResult.VERSION_CONFLICT);
    assertTrue(
        assertThrows(IllegalStateException.class, () -> writer.write("new"))
            .getMessage()
            .contains(id.toString()));
    verify(store, times(5)).getState(id, JobCheckpointWriter.NAMESPACE);
  }

  @Test
  void synchronousSupersededThrowsAndMarksAttempt() {
    when(store.initState(id, JobCheckpointWriter.NAMESPACE, "new", 7L))
        .thenReturn(StateWriteResult.SUPERSEDED);
    assertThrows(AttemptSupersededException.class, () -> writer.write("new"));
    assertTrue(attempt.isSuperseded());
  }

  @Test
  void registrationDoesNoIoAndOnlyCommitChangesCache() {
    registered();
    writer.write("new");
    verifyNoInteractions(store);
    assertEquals(Optional.empty(), writer.read());
    clearInvocations(store);
    commits.get(0).run();
    assertEquals(Optional.of("new"), writer.read());
    verifyNoInteractions(store);
    writer.flush();
    verify(store).initState(id, JobCheckpointWriter.NAMESPACE, "new", 7L);
  }

  @Test
  void deferredSupersededRevertsCacheAndClosesWriter() {
    writer.write("durable");
    registered();
    writer.write("pending");
    commits.get(1).run();
    when(store.updateState(id, JobCheckpointWriter.NAMESPACE, "pending", 0, 7L))
        .thenReturn(StateWriteResult.SUPERSEDED);
    writer.flush();
    assertEquals(Optional.of("durable"), writer.read());
    assertFalse(writer.hasUnflushed());
    assertThrows(AttemptSupersededException.class, () -> writer.write("later"));
    verify(flusher).remove(writer);
  }

  @Test
  void deferredSupersededWithoutDurableValueClearsCache() {
    registered();
    writer.write("pending");
    commits.get(0).run();
    when(store.initState(id, JobCheckpointWriter.NAMESPACE, "pending", 7L))
        .thenReturn(StateWriteResult.SUPERSEDED);
    writer.flush();
    assertEquals(Optional.empty(), writer.read());
  }

  @Test
  void rollbackOnlyKeepsCacheAndDoesNotWrite() {
    writer.write("old");
    clearInvocations(store);
    doReturn(Result.ROLLBACK_ONLY).when(registrar).registerAfterCommit(any());
    writer.write("new");
    assertEquals(Optional.of("old"), writer.read());
    verifyNoInteractions(store);
  }

  @Test
  void registrationFailureAndExceptionsPropagate() {
    doReturn(Result.REGISTRATION_FAILED).when(registrar).registerAfterCommit(any());
    assertThrows(IllegalStateException.class, () -> writer.write("new"));
    RuntimeException failure = new RuntimeException("registrar");
    doThrow(failure).when(registrar).registerAfterCommit(any());
    assertSame(failure, assertThrows(RuntimeException.class, () -> writer.write("new")));
    verifyNoInteractions(store);
  }

  @Test
  void unsupportedStoreFailsForBothOperations() {
    writer = new JobCheckpointWriter(id, attempt, null, registrar, serializer, flusher);
    assertThrows(UnsupportedOperationException.class, () -> writer.write("new"));
    assertThrows(UnsupportedOperationException.class, writer::read);
    verifyNoInteractions(registrar);
  }

  @Test
  void readsOnceAndRejectsMalformedValues() {
    when(store.getState(id, JobCheckpointWriter.NAMESPACE))
        .thenReturn(Optional.of(new VersionedState("json", 3)));
    when(serializer.deserialize("json", Map.class)).thenReturn(Map.of("value", "saved"));
    assertEquals(Optional.of("saved"), writer.read());
    assertEquals(Optional.of("saved"), writer.read());
    verify(store).getState(id, JobCheckpointWriter.NAMESPACE);
    writer.write("next");
    verify(store).updateState(id, JobCheckpointWriter.NAMESPACE, "next", 3, 7L);
    JobCheckpointWriter other =
        new JobCheckpointWriter(id, attempt, store, registrar, serializer, flusher);
    when(serializer.deserialize("json", Map.class)).thenReturn(Map.of("value", 2));
    assertTrue(
        assertThrows(IllegalStateException.class, other::read)
            .getMessage()
            .contains(id.toString()));
  }

  @Test
  void outOfOrderCommitsNeverOverwriteNewerValue() {
    registered();
    writer.write("outer");
    writer.write("inner");
    commits.get(1).run();
    writer.flush();
    commits.get(0).run();
    writer.flush();
    assertEquals(Optional.of("inner"), writer.read());
    verify(store).initState(id, JobCheckpointWriter.NAMESPACE, "inner", 7L);
    verify(store, never()).updateState(any(), anyString(), anyString(), anyInt(), anyLong());
  }

  @Test
  void lateCommitAfterCloseIsIgnored() {
    registered();
    writer.write("late");
    writer.close();
    writer.close();
    commits.get(0).run();
    writer.flush();
    verify(flusher, never()).markDirty(any());
    verifyNoInteractions(store);
  }

  @Test
  void realSerializerRoundTripAndSizeCheckBeforeRegistration() {
    JsonbPayloadSerializer real = new JsonbPayloadSerializer();
    writer = new JobCheckpointWriter(id, attempt, store, registrar, real, flusher);
    String value = "quotes \" and backslash \\ and newline\n and 日本語";
    writer.write(value);
    var json = ArgumentCaptor.forClass(String.class);
    verify(store).initState(eq(id), eq(JobCheckpointWriter.NAMESPACE), json.capture(), eq(7L));
    when(store.getState(id, JobCheckpointWriter.NAMESPACE))
        .thenReturn(Optional.of(new VersionedState(json.getValue(), 0)));
    JobCheckpointWriter reader =
        new JobCheckpointWriter(id, attempt, store, registrar, real, flusher);
    assertEquals(Optional.of(value), reader.read());
    clearInvocations(registrar);
    assertThrows(
        IllegalArgumentException.class,
        () -> writer.write("x".repeat(ExtensionValidation.MAX_STATE_BYTES)));
    verifyNoInteractions(registrar);
  }

  private void registered() {
    doAnswer(
            call -> {
              commits.add(call.getArgument(0));
              return Result.REGISTERED;
            })
        .when(registrar)
        .registerAfterCommit(any());
  }
}
