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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Result;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;

class CheckpointFlusherTest {
  private final CheckpointFlusher flusher = new CheckpointFlusher(RatchetOptions.builder().build());
  private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
  private final ScheduledFuture<?> handle = mock(ScheduledFuture.class);
  private final List<Runnable> scheduled = new ArrayList<>();

  @BeforeEach
  void setup() {
    doAnswer(
            call -> {
              scheduled.add(call.getArgument(0));
              return handle;
            })
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  void idleNodeNeverSchedulesAndBurstSchedulesExactlyOneOneShot() {
    flusher.start(executor);
    verifyNoInteractions(executor);
    WriterFixture fixture = writer();
    flusher.markDirty(fixture.writer());
    flusher.markDirty(fixture.writer());
    flusher.markDirty(fixture.writer());
    verify(executor).schedule(any(Runnable.class), eq(1000L), eq(TimeUnit.MILLISECONDS));
    scheduled.get(0).run();
    verifyWritten(fixture);
    assertFalse(fixture.writer().hasUnflushed());
    assertEquals(1, scheduled.size());
  }

  @Test
  void beforeStartAndAfterStopOnlyRecordAndShutdownCanStillDrain() {
    WriterFixture fixture = writer();
    verifyNoInteractions(executor);
    flusher.start(executor);
    flusher.stop();
    verify(handle).cancel(false);
    WriterFixture late = writer();
    assertEquals(1, scheduled.size());
    flusher.flushAll();
    verifyWritten(fixture);
    verifyWritten(late);
  }

  @Test
  void markDuringFlushIsNotLostAndSchedulesExactlyOneMoreRun() {
    WriterFixture fixture = writer();
    when(fixture.store().initState(eq(fixture.writer().jobId()), anyString(), anyString(), eq(7L)))
        .thenAnswer(
            call -> {
              flusher.markDirty(fixture.writer());
              return StateWriteResult.UPDATED;
            });
    flusher.start(executor);
    scheduled.get(0).run();
    assertEquals(2, scheduled.size());
    verifyWritten(fixture);
    scheduled.get(1).run();
    assertEquals(2, scheduled.size());
    verifyWritten(fixture);
  }

  @Test
  void throwingWriterIsReaddedWithoutStoppingOthers() {
    WriterFixture bad = writer();
    WriterFixture good = writer();
    when(bad.store().initState(eq(bad.writer().jobId()), anyString(), anyString(), eq(7L)))
        .thenThrow(new IllegalStateException("store down"));
    flusher.start(executor);
    scheduled.get(0).run();
    verifyWritten(good);
    verifyWritten(bad);
    assertTrue(bad.writer().hasUnflushed());
    assertEquals(2, scheduled.size());
    flusher.remove(bad.writer());
    scheduled.get(1).run();
    assertEquals(2, scheduled.size());
  }

  @Test
  void batchIsBoundedAndFlushAllDrainsRemainder() {
    List<WriterFixture> writers = new ArrayList<>();
    for (int i = 0; i < 105; i++) writers.add(writer());
    flusher.start(executor);
    scheduled.get(0).run();
    long count = writers.stream().filter(w -> !w.writer().hasUnflushed()).count();
    assertTrue(count <= 100);
    assertTrue(count > 0);
    assertEquals(2, scheduled.size());
    flusher.flushAll();
    for (WriterFixture fixture : writers) {
      verifyWritten(fixture);
      assertFalse(fixture.writer().hasUnflushed());
    }
  }

  @Test
  void removeDeregistersWriter() {
    WriterFixture fixture = writer();
    flusher.remove(fixture.writer());
    flusher.start(executor);
    flusher.flushAll();
    verify(fixture.store(), never()).getState(any(), anyString());
    verify(fixture.store(), never()).initState(any(), anyString(), anyString(), anyLong());
    verifyNoInteractions(executor);
  }

  @Test
  void pendingAfterSuccessfulFlushIsReenqueued() {
    WriterFixture fixture = writer();
    when(fixture.store().initState(eq(fixture.writer().jobId()), anyString(), anyString(), eq(7L)))
        .thenAnswer(
            call -> {
              fixture.writer().write("newer");
              fixture.commits().get(1).run();
              return StateWriteResult.UPDATED;
            });
    when(fixture
            .store()
            .updateState(eq(fixture.writer().jobId()), anyString(), anyString(), eq(0), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    flusher.start(executor);
    scheduled.get(0).run();
    assertEquals(2, scheduled.size());
    assertTrue(fixture.writer().hasUnflushed());
    scheduled.get(1).run();
    verifyWritten(fixture);
    verify(fixture.store())
        .updateState(
            eq(fixture.writer().jobId()),
            eq(JobCheckpointWriter.NAMESPACE),
            anyString(),
            eq(0),
            eq(7L));
    assertFalse(fixture.writer().hasUnflushed());
    assertEquals(Optional.of("newer"), fixture.writer().read());
    assertEquals(2, scheduled.size());
  }

  private void verifyWritten(WriterFixture fixture) {
    verify(fixture.store())
        .initState(
            eq(fixture.writer().jobId()), eq(JobCheckpointWriter.NAMESPACE), anyString(), eq(7L));
  }

  private WriterFixture writer() {
    UUID id = UUID.randomUUID();
    JobStateStore store = mock(JobStateStore.class);
    AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
    List<Runnable> commits = new ArrayList<>();
    when(registrar.registerAfterCommit(any()))
        .thenAnswer(
            call -> {
              commits.add(call.getArgument(0));
              return Result.REGISTERED;
            });
    when(store.getState(id, JobCheckpointWriter.NAMESPACE)).thenReturn(Optional.empty());
    when(store.initState(eq(id), anyString(), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    JobAttemptControl attempt =
        new JobAttemptControl(id, Instant.EPOCH.plusSeconds(30), 30, Instant.EPOCH, 0, 7L, "node");
    JobCheckpointWriter writer =
        new JobCheckpointWriter(
            id, attempt, store, registrar, new JsonbTestPayloadSerializer(), flusher);
    writer.write("pending");
    commits.get(0).run();
    return new WriterFixture(writer, store, commits);
  }

  private record WriterFixture(
      JobCheckpointWriter writer, JobStateStore store, List<Runnable> commits) {}
}
