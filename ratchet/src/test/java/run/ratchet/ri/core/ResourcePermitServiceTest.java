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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.ri.core.internal.DefaultResourcePermitService;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

@ExtendWith(MockitoExtension.class)
class ResourcePermitServiceTest {

  @Mock private ResourcePermitStore resourcePermitStore;
  @Mock private PollerScheduler pollerScheduler;

  @Test
  void release_wakesPollerWhenStoreReleaseFails() {
    DefaultResourcePermitService service =
        new DefaultResourcePermitService(resourcePermitStore, pollerScheduler);
    UUID jobId = new UUID(0L, 1L);
    doThrow(new RuntimeException("store unavailable"))
        .when(resourcePermitStore)
        .releasePermit("api", jobId, 0L);

    assertThrows(RuntimeException.class, () -> service.release("api", jobId, 0L));

    verify(pollerScheduler).wakeup();
  }

  @Test
  void releaseAll_wakesPollerWhenStoreReleaseFails() {
    DefaultResourcePermitService service =
        new DefaultResourcePermitService(resourcePermitStore, pollerScheduler);
    UUID jobId = new UUID(0L, 2L);
    doThrow(new RuntimeException("store unavailable"))
        .when(resourcePermitStore)
        .releaseAllPermits(jobId);

    assertThrows(RuntimeException.class, () -> service.releaseAll(jobId));

    verify(pollerScheduler).wakeup();
  }

  @Test
  void acquireAndRelease_passClaimToStore() {
    DefaultResourcePermitService service =
        new DefaultResourcePermitService(resourcePermitStore, pollerScheduler);
    UUID jobId = UUID.randomUUID();
    when(resourcePermitStore.tryAcquirePermit("api", jobId, 7L, "node"))
        .thenReturn(AcquireResult.SUPERSEDED);
    assertEquals(AcquireResult.SUPERSEDED, service.tryAcquire("api", jobId, 7L, "node"));
    verify(resourcePermitStore).tryAcquirePermit("api", jobId, 7L, "node");
    service.release("api", jobId, 7L);
    verify(resourcePermitStore).releasePermit("api", jobId, 7L);
  }

  @Test
  void missingStore_grantsPermit() {
    DefaultResourcePermitService service =
        new DefaultResourcePermitService((ResourcePermitStore) null, pollerScheduler);
    assertEquals(AcquireResult.ACQUIRED, service.tryAcquire("api", UUID.randomUUID(), 7L, "node"));
  }
}
