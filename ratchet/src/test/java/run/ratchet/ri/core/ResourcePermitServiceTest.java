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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.api.ResourceLimit;
import run.ratchet.ri.core.internal.DefaultResourcePermitService;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Result;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourcePermitStore.WokenWaiter;

class ResourcePermitServiceTest {
  private final ResourcePermitStore store = mock(ResourcePermitStore.class);
  private final JobWakeupService wakeup = mock(JobWakeupService.class);
  private final UUID jobId = UUID.randomUUID();

  private static class Registrar implements AfterCommitRegistrar {
    private final List<Runnable> callbacks = new ArrayList<>();
    private final Result result;

    Registrar(Result result) {
      this.result = result;
    }

    @Override
    public Result registerAfterCommit(Runnable callback) {
      callbacks.add(callback);
      return result;
    }

    void commit() {
      callbacks.forEach(Runnable::run);
    }
  }

  @Test
  void mutationsWakeOnlyAfterCommitOrImmediatelyWithoutTransaction() {
    for (Result result : List.of(Result.REGISTERED, Result.NO_ACTIVE_TRANSACTION)) {
      List<Consumer<DefaultResourcePermitService>> mutations =
          List.of(
              service -> service.release("api", jobId, 7L),
              service -> service.releaseAll(jobId),
              service -> service.configureResource("api", 10, 100, null),
              service -> service.cleanupOrphanedPermits(List.of("stale")));
      for (var mutation : mutations) {
        reset(store, wakeup);
        when(store.cleanupOrphanedPermits(List.of("stale"))).thenReturn(1);
        when(store.findResourcesWithWaiters(null, 64)).thenReturn(List.of("api"));
        when(store.wakeWaiters("api"))
            .thenReturn(
                List.of(
                    new WokenWaiter(jobId, JobPriority.HIGH.persistedCode(), "workers"),
                    new WokenWaiter(UUID.randomUUID(), JobPriority.NORMAL.persistedCode(), null)));
        Registrar registrar = new Registrar(result);
        var service = new DefaultResourcePermitService(store, wakeup, registrar);
        mutation.accept(service);
        if (result == Result.REGISTERED) {
          verify(store, never()).wakeWaiters(anyString());
          verifyNoInteractions(wakeup);
          registrar.commit();
        }
        verify(store).wakeWaiters("api");
        verify(wakeup).notify(JobPriority.HIGH, true, "workers");
        verify(wakeup).notify(JobPriority.NORMAL, true, null);
      }
    }
  }

  @Test
  void rollbackOrRegistrationFailureSkipsWake() {
    for (Result result : List.of(Result.ROLLBACK_ONLY, Result.REGISTRATION_FAILED)) {
      var service = new DefaultResourcePermitService(store, wakeup, new Registrar(result));
      service.release("api", jobId, 7L);
    }
    verify(store, never()).wakeWaiters(anyString());
    verifyNoInteractions(wakeup);
  }

  @Test
  void wakeFailureDoesNotFailRelease() {
    when(store.wakeWaiters("api")).thenThrow(new IllegalStateException("unavailable"));
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));
    assertDoesNotThrow(() -> service.release("api", jobId, 7L));
  }

  @Test
  void nonemptySweepDeletesStaleWaitersAfterWaking() {
    when(store.findResourcesWithWaiters(null, 64)).thenReturn(List.of("api", "gpu"));
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));
    service.sweepWaiters();
    verify(store).wakeWaiters("api");
    verify(store).wakeWaiters("gpu");
    verify(store).deleteStaleWaiters(500);
  }

  @Test
  void failedReleaseDoesNotWake() {
    doThrow(new IllegalStateException("release failed"))
        .when(store)
        .releasePermit("api", jobId, 7L);
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));
    assertThrows(IllegalStateException.class, () -> service.release("api", jobId, 7L));
    verify(store, never()).wakeWaiters(anyString());
    verifyNoInteractions(wakeup);
  }

  @Test
  void fullSweepPageAdvancesCursorAndShortPageResetsIt() {
    List<String> fullPage =
        IntStream.range(0, 64).mapToObj(i -> "resource-%02d".formatted(i)).toList();
    when(store.findResourcesWithWaiters(null, 64)).thenReturn(fullPage, List.of());
    when(store.findResourcesWithWaiters("resource-63", 64)).thenReturn(List.of("resource-64"));
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));

    service.sweepWaiters();
    service.sweepWaiters();
    service.sweepWaiters();

    var order = inOrder(store);
    order.verify(store).findResourcesWithWaiters(null, 64);
    for (String resource : fullPage) {
      order.verify(store).wakeWaiters(resource);
    }
    order.verify(store).deleteStaleWaiters(500);
    order.verify(store).findResourcesWithWaiters("resource-63", 64);
    order.verify(store).wakeWaiters("resource-64");
    order.verify(store).deleteStaleWaiters(500);
    order.verify(store).findResourcesWithWaiters(null, 64);
    verifyNoMoreInteractions(store);
  }

  @Test
  void emptyPageAfterFullPageResetsCursor() {
    List<String> fullPage =
        IntStream.range(0, 64).mapToObj(i -> "resource-%02d".formatted(i)).toList();
    when(store.findResourcesWithWaiters(null, 64)).thenReturn(fullPage, List.of());
    when(store.findResourcesWithWaiters("resource-63", 64)).thenReturn(List.of());
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));

    service.sweepWaiters();
    service.sweepWaiters();
    service.sweepWaiters();

    var order = inOrder(store);
    order.verify(store).findResourcesWithWaiters(null, 64);
    for (String resource : fullPage) {
      order.verify(store).wakeWaiters(resource);
    }
    order.verify(store).deleteStaleWaiters(500);
    order.verify(store).findResourcesWithWaiters("resource-63", 64);
    order.verify(store).findResourcesWithWaiters(null, 64);
    verifyNoMoreInteractions(store);
  }

  @Test
  void emptySweepMakesOnlyOneRead() {
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));
    service.sweepWaiters();
    verify(store).findResourcesWithWaiters(null, 64);
    verifyNoMoreInteractions(store);
    verifyNoInteractions(wakeup);
  }

  @Test
  void acquirePassesClaimToStore() {
    when(store.tryAcquirePermit("api", jobId, 7L, "node")).thenReturn(AcquireResult.SUPERSEDED);
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));
    assertEquals(AcquireResult.SUPERSEDED, service.tryAcquire("api", jobId, 7L, "node"));
  }

  @Test
  void absentStoreDisablesAllWork() {
    AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
    var service = new DefaultResourcePermitService((ResourcePermitStore) null, wakeup, registrar);
    assertEquals(AcquireResult.ACQUIRED, service.tryAcquire("api", jobId, 7L, "node"));
    assertEquals(5000, service.getRetryDelay("api"));
    service.release("api", jobId, 7L);
    service.releaseAll(jobId);
    assertEquals(0, service.cleanupOrphanedPermits(List.of("node")));
    service.sweepWaiters();
    assertEquals(Optional.empty(), service.findResourceLimit("api"));
    assertEquals(List.of(), service.listResourceLimits());
    verifyNoInteractions(store, wakeup, registrar);
  }

  @Test
  void absentStoreRejectsConfigureResource() {
    // A limit that cannot be stored must not look as if it was set.
    AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
    var service = new DefaultResourcePermitService((ResourcePermitStore) null, wakeup, registrar);

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> service.configureResource("api", 10, 100, null));

    assertEquals(
        "Resource limits require a store that advertises the ResourcePermitStore capability",
        exception.getMessage());
    verifyNoInteractions(wakeup, registrar);
  }

  @Test
  void findResourceLimitReadsTheStore() {
    ResourceLimit limit = new ResourceLimit("api", 2, Duration.ofMillis(100), "API slots");
    when(store.findResourceLimit("api")).thenReturn(Optional.of(limit));
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));

    assertEquals(Optional.of(limit), service.findResourceLimit("api"));
    assertEquals(Optional.empty(), service.findResourceLimit("gpu"));
    verifyNoInteractions(wakeup);
  }

  @Test
  void listResourceLimitsSortsByNameAndCannotBeChanged() {
    ResourceLimit gpu = ResourceLimit.of("gpu", 1);
    ResourceLimit api = ResourceLimit.of("api", 2);
    ResourceLimit upper = ResourceLimit.of("Zeta", 3);
    when(store.listResourceLimits()).thenReturn(new ArrayList<>(List.of(gpu, upper, api)));
    var service =
        new DefaultResourcePermitService(
            store, wakeup, new Registrar(Result.NO_ACTIVE_TRANSACTION));

    List<ResourceLimit> limits = service.listResourceLimits();

    assertEquals(List.of(upper, api, gpu), limits);
    assertThrows(UnsupportedOperationException.class, () -> limits.add(gpu));
  }
}
