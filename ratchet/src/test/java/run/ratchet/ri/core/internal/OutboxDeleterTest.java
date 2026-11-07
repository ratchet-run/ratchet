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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.store.spi.EventOutboxStore;

class OutboxDeleterTest {
  @Test
  void idleHasNoWakeupsAndEnqueuesCoalesceIntoBoundedBatches() {
    EventOutboxStore store = mock(EventOutboxStore.class);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doReturn(mock(ScheduledFuture.class))
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    OutboxDeleter deleter = new OutboxDeleter(store);
    deleter.start(executor);
    verifyNoInteractions(executor);
    List<UUID> ids = IntStream.range(0, 1001).mapToObj(i -> UUID.randomUUID()).toList();
    deleter.enqueue(ids.subList(0, 500));
    deleter.enqueue(ids.subList(500, 1001));
    verifyNoInteractions(store);
    ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
    verify(executor)
        .schedule(callback.capture(), eq(OutboxDeleter.COALESCE_MS), eq(TimeUnit.MILLISECONDS));
    callback.getValue().run();
    var order = inOrder(store);
    order.verify(store).deleteEvents(ids.subList(0, 500));
    order.verify(store).deleteEvents(ids.subList(500, 1000));
    order.verify(store).deleteEvents(ids.subList(1000, 1001));
    verifyNoMoreInteractions(executor);
    deleter.stop();
    verifyNoMoreInteractions(store);
  }

  @Test
  void failedDeletesRetainOnlyTheFirstHundredThousandIdsAndAcceptIdsAfterDraining() {
    EventOutboxStore store = mock(EventOutboxStore.class);
    OutboxDeleter deleter = new OutboxDeleter(store);
    List<UUID> ids = IntStream.range(0, 100_002).mapToObj(i -> new UUID(0, i)).toList();
    deleter.enqueue(ids.subList(0, 100_000));
    when(store.deleteEvents(any())).thenThrow(new IllegalStateException("offline"));
    deleter.drain();
    deleter.enqueue(ids);
    clearInvocations(store);
    doReturn(500).when(store).deleteEvents(any());
    deleter.drain();
    ArgumentCaptor<Collection<UUID>> deleted = ArgumentCaptor.forClass(Collection.class);
    verify(store, times(200)).deleteEvents(deleted.capture());
    assertEquals(
        ids.subList(0, 100_000),
        deleted.getAllValues().stream().flatMap(Collection::stream).toList());
    clearInvocations(store);
    deleter.enqueue(ids.subList(100_000, 100_002));
    deleter.drain();
    verify(store).deleteEvents(ids.subList(100_000, 100_002));
  }

  @Test
  void failedBatchIsRetainedAndShutdownMakesFinalAttempt() {
    EventOutboxStore store = mock(EventOutboxStore.class);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doReturn(mock(ScheduledFuture.class))
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    List<UUID> ids = List.of(UUID.randomUUID());
    when(store.deleteEvents(ids)).thenThrow(new IllegalStateException("offline")).thenReturn(1);
    OutboxDeleter deleter = new OutboxDeleter(store);
    deleter.start(executor);
    deleter.enqueue(ids);
    deleter.drain();
    verify(executor, times(2)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    deleter.stop();
    verify(store, times(2)).deleteEvents(ids);
  }

  @Test
  void scheduledRetryDeletesTheSameFailedIdsAndReturnsToIdle() {
    EventOutboxStore store = mock(EventOutboxStore.class);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doReturn(mock(ScheduledFuture.class))
        .when(executor)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    List<UUID> ids = List.of(UUID.randomUUID());
    when(store.deleteEvents(ids)).thenThrow(new IllegalStateException("offline")).thenReturn(1);
    OutboxDeleter deleter = new OutboxDeleter(store);
    deleter.start(executor);
    deleter.enqueue(ids);
    ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
    verify(executor).schedule(callback.capture(), anyLong(), any(TimeUnit.class));
    callback.getValue().run();
    verify(executor).schedule(any(Runnable.class), eq(2000L), eq(TimeUnit.MILLISECONDS));
    verify(executor, times(2)).schedule(callback.capture(), anyLong(), any(TimeUnit.class));
    callback.getValue().run();
    verify(store, times(2)).deleteEvents(ids);
    verifyNoMoreInteractions(executor);
  }
}
