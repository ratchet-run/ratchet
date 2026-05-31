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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.internal.SingletonLeaseService;
import run.ratchet.store.spi.LockStore;

/** Handle for an acquired cluster-wide singleton lease. */
public final class SingletonLease implements AutoCloseable {

  private static final Logger log = Logger.getLogger(SingletonLease.class);

  private final LockStore lockStore;
  private final String name;
  private final String ownerNode;
  private final AtomicBoolean closed = new AtomicBoolean();

  public SingletonLease(LockStore lockStore, String name, String ownerNode) {
    this.lockStore = Objects.requireNonNull(lockStore, "lockStore must not be null");
    this.name = Objects.requireNonNull(name, "name must not be null");
    this.ownerNode = Objects.requireNonNull(ownerNode, "ownerNode must not be null");
  }

  public String name() {
    return name;
  }

  public String ownerNode() {
    return ownerNode;
  }

  public boolean renew(Duration extension) {
    SingletonLeaseService.requirePositiveDuration(extension, "extension");
    if (closed.get()) {
      return false;
    }
    return lockStore.renewLock(name, extension, ownerNode);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      lockStore.unlock(name, ownerNode);
    } catch (Exception e) {
      log.warnf(e, "Failed to release singleton lease %s for node %s", name, ownerNode);
    }
  }
}
