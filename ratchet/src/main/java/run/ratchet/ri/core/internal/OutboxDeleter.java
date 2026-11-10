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
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.store.spi.EventOutboxStore;

/**
 * Event-driven, bounded deletion outside transaction completion callbacks. Ids that arrive within
 * {@value #COALESCE_MS} ms share one drain. Each scheduled drain deletes at most ten batches before
 * yielding to other executor tasks, and there are no wakeups while idle.
 */
@ApplicationScoped
public class OutboxDeleter {
  private static final Logger log = Logger.getLogger(OutboxDeleter.class);
  private static final int MAX_PENDING = 100_000;
  private static final int BATCH_SIZE = 500;
  private static final int MAX_BATCHES_PER_DRAIN = 10;
  static final long COALESCE_MS = 100;
  private final EventOutboxStore store;
  private final Set<UUID> pending = new LinkedHashSet<>();
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> scheduled;
  private boolean running;
  private boolean stopped;
  private long backoffMs = 1000;
  private boolean outage;
  private boolean full;

  protected OutboxDeleter() {
    this.store = null;
  }

  @Inject
  public OutboxDeleter(Instance<EventOutboxStore> store) {
    this(store.isResolvable() ? store.get() : null);
  }

  public OutboxDeleter(EventOutboxStore store) {
    this.store = store;
  }

  public synchronized void start(ScheduledExecutorService executor) {
    this.executor = executor;
    schedule(0);
  }

  public synchronized void enqueue(Collection<UUID> ids) {
    if (stopped) return;
    for (UUID id : ids) {
      if (pending.contains(id)) continue;
      if (pending.size() < MAX_PENDING) {
        pending.add(id);
      } else {
        if (!full) log.warn("Outbox deletion queue full; new ids remain for the relay");
        else log.debug("Outbox deletion queue still full; new ids remain for the relay");
        full = true;
      }
    }
    schedule(COALESCE_MS);
  }

  private void schedule(long delayMs) {
    if (!stopped && !running && scheduled == null && executor != null && !pending.isEmpty()) {
      try {
        scheduled = executor.schedule(this::drain, delayMs, TimeUnit.MILLISECONDS);
      } catch (RuntimeException failure) {
        log.warn("Cannot schedule outbox deletion; rows remain for the relay", failure);
      }
    }
  }

  void drain() {
    synchronized (this) {
      scheduled = null;
      if (running || stopped) return;
      running = true;
    }
    boolean success;
    try {
      success = drainBatches(MAX_BATCHES_PER_DRAIN);
    } finally {
      synchronized (this) {
        running = false;
        notifyAll();
      }
    }
    synchronized (this) {
      schedule(success ? COALESCE_MS : backoffMs);
    }
  }

  private boolean drainBatches(int maxBatches) {
    for (int i = 0; i < maxBatches; i++) {
      List<UUID> batch;
      synchronized (this) {
        batch = pending.stream().limit(BATCH_SIZE).toList();
      }
      if (batch.isEmpty()) return true;
      try {
        store.deleteEvents(batch);
        synchronized (this) {
          pending.removeAll(batch);
          if (pending.size() < MAX_PENDING) full = false;
          outage = false;
          backoffMs = 1000;
        }
      } catch (RuntimeException failure) {
        synchronized (this) {
          if (!outage) log.warn("Outbox deletion failed; retrying", failure);
          else log.debug("Outbox deletion still unavailable", failure);
          outage = true;
          backoffMs = Math.min(60000, backoffMs * 2);
        }
        return false;
      }
    }
    return true;
  }

  public void stop() {
    synchronized (this) {
      stopped = true;
      if (scheduled != null) scheduled.cancel(false);
      scheduled = null;
      while (running) {
        try {
          wait();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
    // Enqueue is stopped, so a final attempt can cover the entire bounded queue.
    drainBatches(MAX_PENDING / BATCH_SIZE);
  }
}
