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
package run.ratchet.quarkus.it.tck;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.tck.api.ProbeEvent;
import run.ratchet.tck.api.RatchetTckProbe;

/**
 * Quarkus-side {@link RatchetTckProbe}. It mirrors the RI TCK probe, but is discovered as a normal
 * ArC test bean and subscribes to the live Quarkus scheduler through {@link
 * JobSchedulerService#addEventListener(Consumer)}.
 */
@ApplicationScoped
public class QuarkusRatchetTckProbe implements RatchetTckProbe {

  private static final int PENDING_BUFFER_LIMIT = 256;

  private final Map<UUID, Deque<ProbeEvent>> recordedEvents = new ConcurrentHashMap<>();
  private final Map<UUID, Deque<ProbeEvent>> pendingEvents = new ConcurrentHashMap<>();
  private final Set<UUID> trackedIds = ConcurrentHashMap.newKeySet();
  private final Object stateLock = new Object();

  @Inject JobSchedulerService scheduler;

  private Consumer<Object> listener;

  @PostConstruct
  void registerListener() {
    listener = this::observe;
    scheduler.addEventListener(listener);
  }

  @PreDestroy
  void unregisterListener() {
    if (listener != null) {
      scheduler.removeEventListener(listener);
      listener = null;
    }
  }

  public void reset() {
    synchronized (stateLock) {
      trackedIds.clear();
      recordedEvents.clear();
      pendingEvents.clear();
    }
  }

  @Override
  public void track(JobHandle handle) {
    UUID id = handle.id();
    synchronized (stateLock) {
      if (!trackedIds.add(id)) {
        return;
      }
      Deque<ProbeEvent> pending = pendingEvents.remove(id);
      if (pending != null) {
        Deque<ProbeEvent> bucket =
            recordedEvents.computeIfAbsent(id, k -> new ConcurrentLinkedDeque<>());
        recordAllAndNotify(bucket, pending);
      }
    }
  }

  @Override
  public boolean awaitExecuted(JobHandle handle, Duration timeout) {
    return awaitType(handle, ProbeEvent.Type.STARTED, timeout);
  }

  @Override
  public boolean awaitCompleted(JobHandle handle, Duration timeout) {
    return awaitType(handle, ProbeEvent.Type.COMPLETED, timeout);
  }

  @Override
  public boolean awaitFailed(JobHandle handle, Duration timeout) {
    return awaitType(handle, ProbeEvent.Type.FAILED, timeout);
  }

  @Override
  public boolean awaitCancelled(JobHandle handle, Duration timeout) {
    return awaitType(handle, ProbeEvent.Type.CANCELLED, timeout);
  }

  @Override
  public int invocationCount(JobHandle handle) {
    Deque<ProbeEvent> bucket = recordedEvents.get(handle.id());
    if (bucket == null) {
      return 0;
    }
    synchronized (bucket) {
      int count = 0;
      for (ProbeEvent event : bucket) {
        if (event.type() == ProbeEvent.Type.STARTED) {
          count++;
        }
      }
      return count;
    }
  }

  @Override
  public List<ProbeEvent> events(JobHandle handle) {
    Deque<ProbeEvent> bucket = recordedEvents.get(handle.id());
    if (bucket == null) {
      return Collections.emptyList();
    }
    synchronized (bucket) {
      return List.copyOf(new ArrayList<>(bucket));
    }
  }

  private static void recordAndNotify(Deque<ProbeEvent> bucket, ProbeEvent event) {
    synchronized (bucket) {
      bucket.add(event);
      bucket.notifyAll();
    }
  }

  private static void recordAllAndNotify(Deque<ProbeEvent> bucket, Deque<ProbeEvent> events) {
    synchronized (bucket) {
      bucket.addAll(events);
      bucket.notifyAll();
    }
  }

  private boolean awaitType(JobHandle handle, ProbeEvent.Type type, Duration timeout) {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    Deque<ProbeEvent> bucket =
        recordedEvents.computeIfAbsent(handle.id(), k -> new ConcurrentLinkedDeque<>());
    synchronized (bucket) {
      while (!hasType(bucket, type)) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          return false;
        }
        try {
          bucket.wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    return true;
  }

  private static boolean hasType(Deque<ProbeEvent> bucket, ProbeEvent.Type type) {
    for (ProbeEvent event : bucket) {
      if (event.type() == type) {
        return true;
      }
    }
    return false;
  }

  private void observe(Object event) {
    UUID jobId = jobIdOf(event);
    ProbeEvent.Type type = typeOf(event);
    if (jobId == null || type == null) {
      return;
    }

    ProbeEvent probeEvent = new ProbeEvent(type, Instant.now());
    synchronized (stateLock) {
      if (trackedIds.contains(jobId)) {
        Deque<ProbeEvent> bucket =
            recordedEvents.computeIfAbsent(jobId, k -> new ConcurrentLinkedDeque<>());
        recordAndNotify(bucket, probeEvent);
        return;
      }
      Deque<ProbeEvent> pending =
          pendingEvents.computeIfAbsent(jobId, k -> new ConcurrentLinkedDeque<>());
      if (pending.size() < PENDING_BUFFER_LIMIT) {
        pending.add(probeEvent);
      }
    }
  }

  private static UUID jobIdOf(Object event) {
    if (event instanceof JobStartedEvent e) return e.getJobId();
    if (event instanceof JobCompletedEvent e) return e.getJobId();
    if (event instanceof JobFailedEvent e) return e.getJobId();
    if (event instanceof JobDlqEvent e) return e.getJobId();
    if (event instanceof JobCancelledEvent e) return e.getJobId();
    if (event instanceof JobRetryingEvent e) return e.getJobId();
    return null;
  }

  private static ProbeEvent.Type typeOf(Object event) {
    if (event instanceof JobStartedEvent) return ProbeEvent.Type.STARTED;
    if (event instanceof JobCompletedEvent) return ProbeEvent.Type.COMPLETED;
    if (event instanceof JobFailedEvent) return ProbeEvent.Type.FAILED;
    if (event instanceof JobDlqEvent) return ProbeEvent.Type.FAILED;
    if (event instanceof JobCancelledEvent) return ProbeEvent.Type.CANCELLED;
    if (event instanceof JobRetryingEvent) return ProbeEvent.Type.RETRYING;
    return null;
  }
}
