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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.LockStore;

/** Acquires expiring cluster-wide leases for work that must run on at most one node at a time. */
@ApplicationScoped
public class SingletonLeaseService {

  private static final Logger log = Logger.getLogger(SingletonLeaseService.class);

  private final LockStore lockStore;
  private final NodeIdentityProvider nodeIdentityProvider;

  protected SingletonLeaseService() {
    this.lockStore = null;
    this.nodeIdentityProvider = null;
  }

  @Inject
  public SingletonLeaseService(LockStore lockStore, NodeIdentityProvider nodeIdentityProvider) {
    this.lockStore = lockStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
  }

  static String requireLeaseName(String leaseName) {
    String normalized = Objects.requireNonNull(leaseName, "leaseName must not be null").trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("leaseName must not be blank");
    }
    return normalized;
  }

  public static void requirePositiveDuration(Duration duration, String argumentName) {
    Objects.requireNonNull(duration, argumentName + " must not be null");
    if (duration.isZero() || duration.isNegative()) {
      throw new IllegalArgumentException(argumentName + " must be positive");
    }
  }

  public Optional<SingletonLease> tryAcquire(String leaseName, Duration ttl) {
    String normalizedName = requireLeaseName(leaseName);
    requirePositiveDuration(ttl, "ttl");

    String nodeId = nodeIdentityProvider.getNodeId();
    try {
      if (!lockStore.tryLock(normalizedName, ttl, nodeId)) {
        return Optional.empty();
      }
    } catch (RuntimeException e) {
      log.errorf(e, "Failed to acquire singleton lease %s for node %s", normalizedName, nodeId);
      return Optional.empty();
    }

    return Optional.of(new SingletonLease(lockStore, normalizedName, nodeId));
  }
}
