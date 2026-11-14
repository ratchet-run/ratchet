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
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.store.spi.JobCrudStore;

/** Relays committed cancellations to attempts running on this node. */
@ApplicationScoped
public class CancelRequestRelay {
  static final Duration TICK_INTERVAL = Duration.ofSeconds(5);

  private static final Logger log = Logger.getLogger(CancelRequestRelay.class);
  private JobTimeoutHandler timeoutHandler;
  private CancelStatusReader statusReader;
  private InternalEventPublisher eventPublisher;
  private Clock clock;
  private ScheduledFuture<?> handle;
  private boolean started;
  private final Consumer<Object> listener = this::onEvent;

  protected CancelRequestRelay() {}

  @Inject
  public CancelRequestRelay(
      JobTimeoutHandler timeoutHandler,
      CancelStatusReader statusReader,
      InternalEventPublisher eventPublisher,
      Clock clock) {
    this.timeoutHandler = timeoutHandler;
    this.statusReader = statusReader;
    this.eventPublisher = eventPublisher;
    this.clock = clock;
  }

  public synchronized void start(ScheduledExecutorService executor) {
    if (started) return;
    if (eventPublisher != null) eventPublisher.addListener(listener);
    started = true;
    try {
      handle =
          executor.scheduleWithFixedDelay(
              () -> {
                try {
                  tick();
                } catch (RuntimeException e) {
                  log.debug("Cancel relay tick failed", e);
                }
              },
              TICK_INTERVAL.toSeconds(),
              TICK_INTERVAL.toSeconds(),
              TimeUnit.SECONDS);
    } catch (RuntimeException e) {
      started = false;
      if (eventPublisher != null) eventPublisher.removeListener(listener);
      throw e;
    }
  }

  public synchronized void stop() {
    if (!started) return;
    started = false;
    if (eventPublisher != null) eventPublisher.removeListener(listener);
    if (handle != null) {
      handle.cancel(false);
      handle = null;
    }
  }

  private void onEvent(Object event) {
    try {
      if (event instanceof JobCancelledEvent canceled
          && "RUNNING".equals(canceled.getPreviousStatus())) {
        timeoutHandler.requestUserCancel(canceled.getJobId());
      }
    } catch (RuntimeException e) {
      log.debug("Cancel event relay failed", e);
    }
  }

  void tick() {
    var attempts = timeoutHandler.localAttemptsStartedBefore(clock.instant().minus(TICK_INTERVAL));
    if (attempts.isEmpty()) return;
    var ids = new LinkedHashSet<UUID>();
    for (var attempt : attempts) ids.add(attempt.jobId());
    var ordered = new ArrayList<>(ids);
    for (int start = 0; start < ordered.size(); start += JobCrudStore.STATUS_READ_LIMIT) {
      var chunk =
          ordered.subList(start, Math.min(ordered.size(), start + JobCrudStore.STATUS_READ_LIMIT));
      try {
        var statuses = statusReader.findStatuses(chunk);
        for (UUID id : chunk) {
          if (statuses.get(id) == JobStatus.CANCELED) timeoutHandler.requestUserCancel(id);
        }
      } catch (RuntimeException e) {
        log.debug("Cancel status read failed", e);
      }
    }
  }
}
