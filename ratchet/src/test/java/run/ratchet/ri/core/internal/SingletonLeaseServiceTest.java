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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.LockStore;

@ExtendWith(MockitoExtension.class)
class SingletonLeaseServiceTest {

  @Mock private LockStore lockStore;
  @Mock private NodeIdentityProvider nodeIdentityProvider;

  private SingletonLeaseService service;

  @BeforeEach
  void setUp() {
    service = new SingletonLeaseService(lockStore, nodeIdentityProvider);
  }

  @Test
  void tryAcquire_returnsLease_whenStoreLeaseAcquired() {
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(lockStore.tryLock("jobArchiver", Duration.ofMinutes(5), "node-1")).thenReturn(true);

    Optional<SingletonLease> lease = service.tryAcquire(" jobArchiver ", Duration.ofMinutes(5));

    assertTrue(lease.isPresent());
    assertEquals("jobArchiver", lease.get().name());
    assertEquals("node-1", lease.get().ownerNode());
  }

  @Test
  void tryAcquire_returnsEmpty_whenStoreLeaseNotAcquired() {
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    when(lockStore.tryLock("jobArchiver", Duration.ofMinutes(5), "node-1")).thenReturn(false);

    Optional<SingletonLease> lease = service.tryAcquire("jobArchiver", Duration.ofMinutes(5));

    assertTrue(lease.isEmpty());
  }

  @Test
  void tryAcquire_returnsEmpty_whenStoreThrows() {
    when(nodeIdentityProvider.getNodeId()).thenReturn("node-1");
    doThrow(new RuntimeException("db unavailable"))
        .when(lockStore)
        .tryLock("jobArchiver", Duration.ofMinutes(5), "node-1");

    Optional<SingletonLease> lease = service.tryAcquire("jobArchiver", Duration.ofMinutes(5));

    assertTrue(lease.isEmpty());
  }

  @Test
  void renew_delegatesToStoreForAcquiredLease() {
    SingletonLease lease = new SingletonLease(lockStore, "jobArchiver", "node-1");
    when(lockStore.renewLock("jobArchiver", Duration.ofMinutes(5), "node-1")).thenReturn(true);

    assertTrue(lease.renew(Duration.ofMinutes(5)));
  }

  @Test
  void close_releasesLeaseOnlyOnce() {
    SingletonLease lease = new SingletonLease(lockStore, "jobArchiver", "node-1");

    lease.close();
    lease.close();

    verify(lockStore).unlock("jobArchiver", "node-1");
  }

  @Test
  void renew_returnsFalse_afterClose() {
    SingletonLease lease = new SingletonLease(lockStore, "jobArchiver", "node-1");

    lease.close();

    assertFalse(lease.renew(Duration.ofMinutes(5)));
  }

  @Test
  void tryAcquire_rejectsBlankLeaseName() {
    assertThrows(
        IllegalArgumentException.class, () -> service.tryAcquire("  ", Duration.ofMinutes(5)));
  }

  @Test
  void tryAcquire_rejectsNonPositiveTtl() {
    assertThrows(
        IllegalArgumentException.class, () -> service.tryAcquire("jobArchiver", Duration.ZERO));
  }
}
