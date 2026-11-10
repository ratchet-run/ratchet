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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;

class JobProgressWriterTest {
  private final UUID id = UUID.randomUUID();
  private final Instant now = Instant.parse("2026-11-06T12:00:00Z");
  private final JobAttemptControl attempt =
      new JobAttemptControl(id, now.plusSeconds(30), 30, now, 0, 7L, "node");
  private final JobStateStore store = mock(JobStateStore.class);
  private final JobStateFlusher flusher = mock(JobStateFlusher.class);
  private final JsonbTestPayloadSerializer serializer = new JsonbTestPayloadSerializer();
  private final JobProgressWriter writer =
      new JobProgressWriter(
          id, attempt, store, serializer, flusher, Clock.fixed(now, ZoneOffset.UTC));

  private void writable() {
    when(store.getState(id, JobProgressWriter.NAMESPACE)).thenReturn(Optional.empty());
    when(store.initState(eq(id), anyString(), anyString(), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
  }

  @Test
  void reportsCoalesceWithRealJsonAndSteadyStateUsesOneUpdate() {
    writable();
    for (int i = 0; i < 1000; i++) writer.report(i, 100);
    verify(flusher).markDirty(writer);
    writer.flush();
    ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
    verify(store).initState(eq(id), eq(JobProgressWriter.NAMESPACE), json.capture(), eq(7L));
    Map<?, ?> value = serializer.deserialize(json.getValue(), Map.class);
    assertEquals(999L, ((Number) value.get("done")).longValue());
    assertEquals(100L, ((Number) value.get("total")).longValue());
    assertEquals(now.toString(), value.get("reportedAt"));
    assertEquals(3, value.size());
    assertFalse(writer.hasUnflushed());
    when(store.updateState(eq(id), anyString(), anyString(), eq(0), eq(7L)))
        .thenReturn(StateWriteResult.UPDATED);
    writer.report(999, 100);
    writer.flush();
    verify(store).getState(id, JobProgressWriter.NAMESPACE);
    verify(store).updateState(eq(id), eq(JobProgressWriter.NAMESPACE), anyString(), eq(0), eq(7L));
    verify(flusher, times(2)).markDirty(writer);
  }

  @Test
  void supersededWriteMarksAttemptDropsPendingAndCloses() {
    writable();
    doReturn(StateWriteResult.SUPERSEDED)
        .when(store)
        .initState(eq(id), anyString(), anyString(), eq(7L));
    writer.report(1, 2);
    writer.flush();
    assertTrue(attempt.isSuperseded());
    assertFalse(writer.hasUnflushed());
    verify(flusher).remove(writer);
    writer.report(2, 2);
    verify(flusher).markDirty(writer);
  }

  @Test
  void failureRetainsLatestAndRetryCanFlushAfterClose() {
    writable();
    doThrow(new IllegalStateException("store down"))
        .when(store)
        .initState(eq(id), anyString(), anyString(), eq(7L));
    writer.report(1, 2);
    assertThrows(IllegalStateException.class, writer::flush);
    assertTrue(writer.hasUnflushed());
    doReturn(StateWriteResult.UPDATED)
        .when(store)
        .initState(eq(id), anyString(), anyString(), eq(7L));
    writer.close();
    writer.report(2, 2);
    writer.flush();
    verify(store, times(2)).initState(eq(id), anyString(), anyString(), eq(7L));
    verify(flusher).markDirty(writer);
  }

  @Test
  void watchdogFlushDoesNotConsultSupersededFlagButReportsDo() {
    writable();
    writer.report(1, 2);
    attempt.markSuperseded();
    writer.report(2, 2);
    writer.flush();
    verify(store).initState(eq(id), anyString(), anyString(), eq(7L));
    verify(flusher).markDirty(writer);
  }

  @Test
  void reportDuringIoSurvivesAndFailureDoesNotRestoreOlderValue() {
    writable();
    writer.report(1, 2);
    doAnswer(
            call -> {
              writer.report(2, 2);
              throw new IllegalStateException("down");
            })
        .when(store)
        .initState(eq(id), anyString(), anyString(), eq(7L));
    assertThrows(IllegalStateException.class, writer::flush);
    assertTrue(writer.hasUnflushed());
    doReturn(StateWriteResult.UPDATED)
        .when(store)
        .initState(eq(id), anyString(), anyString(), eq(7L));
    writer.flush();
    ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
    verify(store, times(2)).initState(eq(id), anyString(), json.capture(), eq(7L));
    assertEquals(
        2L, ((Number) serializer.deserialize(json.getValue(), Map.class).get("done")).longValue());
  }
}
