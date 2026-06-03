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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import org.jboss.logging.Logger;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.LockStore;

/** Default {@link StartupCoordinator} backed by the store's distributed lock/lease mechanism. */
@ApplicationScoped
class StoreBackedStartupCoordinator implements StartupCoordinator {

  private static final Logger log = Logger.getLogger(StoreBackedStartupCoordinator.class);
  private static final String LOCK_PREFIX = "startup:";

  private final LockStore lockStore;
  private final NodeIdentityProvider nodeIdentityProvider;

  protected StoreBackedStartupCoordinator() {
    this.lockStore = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public StoreBackedStartupCoordinator(
      Instance<LockStore> lockStore, NodeIdentityProvider nodeIdentityProvider) {
    this.lockStore = lockStore.isResolvable() ? lockStore.get() : null;
    this.nodeIdentityProvider = nodeIdentityProvider;
    if (this.lockStore == null) {
      log.info(
          "LockStore capability not advertised by the store — startup coordination degrades to"
              + " single-node semantics (this node always proceeds with one-time startup actions)");
    }
  }

  /** Constructor for tests that supply a lock store directly (or {@code null} to degrade). */
  StoreBackedStartupCoordinator(LockStore lockStore, NodeIdentityProvider nodeIdentityProvider) {
    this.lockStore = lockStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
  }

  private static String lockName(String actionName) {
    String normalized = Objects.requireNonNull(actionName, "actionName must not be null").trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("actionName must not be blank");
    }
    return LOCK_PREFIX + normalized;
  }

  private static Duration positiveLeaseTtl(Duration leaseTtl) {
    Duration ttl = Objects.requireNonNull(leaseTtl, "leaseTtl must not be null");
    if (ttl.isZero() || ttl.isNegative()) {
      throw new IllegalArgumentException("leaseTtl must be positive");
    }
    return ttl;
  }

  @Override
  public boolean tryAcquire(String actionName, Duration leaseTtl) {
    String lockName = lockName(actionName);
    Duration ttl = positiveLeaseTtl(leaseTtl);
    if (lockStore == null) {
      // No distributed lock: this node proceeds with the one-time action (single-node semantics).
      return true;
    }
    return lockStore.tryLock(lockName, ttl, nodeIdentityProvider.getNodeId());
  }

  @Override
  public void release(String actionName) {
    if (lockStore == null) {
      return;
    }
    lockStore.unlock(lockName(actionName), nodeIdentityProvider.getNodeId());
  }
}
