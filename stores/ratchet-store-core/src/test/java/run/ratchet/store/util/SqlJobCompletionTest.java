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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobStore;

class SqlJobCompletionTest {
  @Test
  void unchangedAttemptsDoNotIssueAnUpdate() {
    exerciseAttempts(4, 4);
  }

  @Test
  void changedAttemptsAreWrittenBeforeTerminalTransition() {
    exerciseAttempts(2, 4);
  }

  @Test
  void dependencyPayloadWriterRunsOnlyAfterAllSnapshotGuards() {
    exercisePayloadGuard(false, false);
    exercisePayloadGuard(true, false);
    exercisePayloadGuard(false, true);
  }

  @SuppressWarnings("unchecked")
  private void exercisePayloadGuard(boolean staleDependency, boolean staleBatch) {
    boolean stale = staleDependency || staleBatch;
    EntityManager em = mock(EntityManager.class);
    JobStore store = mock(JobStore.class);
    Query primary = mock(Query.class, RETURNS_SELF);
    Query dependent = mock(Query.class, RETURNS_SELF);
    Query batch = mock(Query.class, RETURNS_SELF);
    Query update = mock(Query.class, RETURNS_SELF);
    UUID id = new UUID(0L, 1L);
    UUID child = new UUID(0L, 2L);
    Instant now = Instant.now();
    when(em.createNativeQuery(startsWith("SELECT "))).thenReturn(primary, dependent, batch);
    when(primary.getResultList())
        .thenReturn(List.<Object[]>of(new Object[] {"RUNNING", 0, now, 1, 1L}));
    when(dependent.getResultList())
        .thenReturn(
            List.<Object[]>of(new Object[] {"PENDING", staleDependency ? 4 : 3, now, 0, 0L}));
    // Another node processed this batch's completion after the plan's snapshot was taken.
    when(batch.getResultList()).thenReturn(List.<Object[]>of(new Object[] {2, 2, 0, 1}));
    when(em.createNativeQuery(startsWith("UPDATE "))).thenReturn(update);
    when(update.executeUpdate()).thenReturn(1);
    when(store.markJobSucceeded(eq(id), any(), any(), any(), any(), any(), any())).thenReturn(true);
    var dependency =
        new JobCompletionPlan.DependencyTransition(
            child,
            JobStatus.PENDING,
            3,
            now,
            JobStatus.PENDING,
            now,
            JobExecutionType.WORKFLOW_BRANCH,
            "{\"args\":[7]}",
            "new-key");
    var plan =
        new JobCompletionPlan(
            id,
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            null,
            null,
            null,
            1,
            now,
            now,
            0L,
            0L,
            null,
            staleBatch ? new JobCompletionPlan.BatchCompletion(2, 2, 0) : null,
            List.of(dependency),
            null,
            List.of());
    Consumer<JobCompletionPlan.DependencyTransition> writer = mock(Consumer.class);
    if (stale) {
      assertThrows(
          RatchetTransientStoreException.class,
          () ->
              SqlJobCompletion.commit(
                  em, store, mock(BatchStore.class), plan, false, uuid -> uuid, writer));
      verify(writer, never()).accept(any());
    } else {
      assertTrue(
          SqlJobCompletion.commit(
                  em, store, mock(BatchStore.class), plan, false, uuid -> uuid, writer)
              .committed());
      verify(writer).accept(dependency);
    }
  }

  private void exerciseAttempts(int stored, int planned) {
    var em = mock(EntityManager.class);
    var store = mock(JobStore.class);
    var locked = mock(Query.class, RETURNS_SELF);
    var update = mock(Query.class, RETURNS_SELF);
    UUID id = UUID.randomUUID();
    Instant now = Instant.now();
    when(em.createNativeQuery(startsWith("SELECT "))).thenReturn(locked);
    when(locked.getResultList())
        .thenReturn(List.<Object[]>of(new Object[] {"RUNNING", 0, now, stored}));
    when(em.createNativeQuery(startsWith("UPDATE "))).thenReturn(update);
    when(store.markJobSucceeded(eq(id), any(), any(), any(), any(), any(), any())).thenReturn(true);
    var plan =
        new JobCompletionPlan(
            id,
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
            null,
            null,
            null,
            planned,
            now,
            now,
            0L,
            0L,
            null,
            null,
            List.of(),
            null,
            List.of());

    assertTrue(
        SqlJobCompletion.commit(em, store, mock(BatchStore.class), plan, false, uuid -> uuid)
            .committed());

    if (stored == planned) {
      verify(update, never()).executeUpdate();
    } else {
      var order = inOrder(update, store);
      order.verify(update).setParameter(1, planned);
      order.verify(update).setParameter(2, id);
      order.verify(update).executeUpdate();
      order.verify(store).markJobSucceeded(id, null, null, now, now, 0L, 0L);
    }
  }
}
