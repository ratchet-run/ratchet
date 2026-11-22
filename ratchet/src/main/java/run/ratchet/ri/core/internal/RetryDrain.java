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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/** One due-ordered, budgeted retry drain for a kind of store write. */
final class RetryDrain {
  static final int MAX_ITEMS = 100;
  static final long TICK_NANOS = Duration.ofSeconds(1).toNanos();
  private static final Logger log = Logger.getLogger(RetryDrain.class);

  enum Submission {
    ACCEPTED,
    DUPLICATE,
    REFUSED
  }

  interface Attempt {
    // null means done; otherwise the absolute monotonic time of the next try.
    Long run();
  }

  interface Refusal {
    void dropped(int count, boolean stopping, RuntimeException failure);

    void retrying(long delayNanos, RuntimeException failure);
  }

  static final class Entry {
    final Object key;
    final Runnable dropped;
    Attempt attempt;
    BiConsumer<Boolean, RuntimeException> refused;
    BiConsumer<Long, RuntimeException> retrying;
    long due;
    long sequence;

    Entry(Object key, Runnable dropped) {
      this.key = key;
      this.dropped = dropped;
    }
  }

  private static final class Ticket {
    final ScheduledExecutorService executor;
    final boolean timer;
    final long due;
    ScheduledFuture<?> future;

    Ticket(ScheduledExecutorService executor, boolean timer, long due) {
      this.executor = executor;
      this.timer = timer;
      this.due = due;
    }
  }

  private final Object lock = new Object();
  private final Map<Object, Entry> pending = new HashMap<>();
  private final PriorityQueue<Entry> queue =
      new PriorityQueue<>(
          (a, b) -> {
            int due = Long.compare(a.due - b.due, 0);
            return due != 0 ? due : Long.compare(a.sequence, b.sequence);
          });
  private final String kind;
  private final Supplier<ScheduledExecutorService> scheduler;
  private final Supplier<ScheduledExecutorService> timer;
  private final LongSupplier clock;
  private final long firstRefusal;
  private final long maxRefusal;
  private final Refusal refusal;
  private Ticket armed;
  private boolean running;
  private long sequence;
  private long refusalDelay;
  private boolean refusalWarned;
  private long refusalWarnedAt;
  private boolean backlogWarned;
  private long backlogWarnedAt;
  private boolean stoppingLogged;

  RetryDrain(
      String kind,
      Supplier<ScheduledExecutorService> scheduler,
      Supplier<ScheduledExecutorService> timer,
      LongSupplier clock,
      Duration firstRefusal,
      Duration maxRefusal,
      Refusal refusal) {
    this.kind = kind;
    this.scheduler = scheduler;
    this.timer = timer;
    this.clock = clock;
    this.firstRefusal = firstRefusal.toNanos();
    this.maxRefusal = maxRefusal.toNanos();
    this.refusal = refusal;
  }

  /** Reserves, queues and arms a hand-off before another caller can observe its key. */
  Submission submit(Object key, long due, Function<Entry, Attempt> attempt, Runnable onDropped) {
    List<Entry> dropped = new ArrayList<>();
    Submission result;
    synchronized (lock) {
      // Recover a dropped ticket before reporting an existing hand-off as a duplicate.
      arm(dropped);
      if (pending.containsKey(key)) result = Submission.DUPLICATE;
      else {
        Entry entry = new Entry(key, onDropped);
        entry.attempt = attempt.apply(entry);
        pending.put(key, entry);
        enqueue(entry, due);
        if (arm(dropped, entry)) result = Submission.ACCEPTED;
        else {
          pending.remove(key, entry);
          dropped.remove(entry);
          result = Submission.REFUSED;
        }
      }
    }
    notifyDropped(dropped);
    return result;
  }

  Entry reserve(Object key, Runnable dropped) {
    List<Entry> abandoned = new ArrayList<>();
    Entry entry;
    synchronized (lock) {
      if (pending.containsKey(key)) entry = null;
      else {
        entry = new Entry(key, dropped);
        pending.put(key, entry);
      }
      arm(abandoned);
    }
    notifyDropped(abandoned);
    return entry;
  }

  void finish(Entry entry) {
    synchronized (lock) {
      pending.remove(entry.key, entry);
    }
  }

  boolean schedule(Entry entry, long due, Attempt attempt) {
    return schedule(entry, due, attempt, null, null);
  }

  boolean schedule(
      Entry entry,
      long due,
      Attempt attempt,
      BiConsumer<Boolean, RuntimeException> refused,
      BiConsumer<Long, RuntimeException> retrying) {
    List<Entry> dropped = new ArrayList<>();
    boolean accepted;
    synchronized (lock) {
      if (pending.get(entry.key) != entry) return false;
      entry.attempt = attempt;
      entry.refused = refused;
      entry.retrying = retrying;
      enqueue(entry, due);
      accepted = arm(dropped, entry);
    }
    // An initial hand-off refusal belongs to the caller, not its dropped callback.
    dropped.remove(entry);
    notifyDropped(dropped);
    return accepted;
  }

  private void enqueue(Entry entry, long due) {
    entry.due = due;
    entry.sequence = sequence++;
    queue.add(entry);
  }

  // Called with lock held. Store attempts and dropped callbacks run outside this lock.
  private boolean arm(List<Entry> dropped) {
    return arm(dropped, null);
  }

  private boolean arm(List<Entry> dropped, Entry initial) {
    if (running || queue.isEmpty()) return true;
    ScheduledExecutorService executor = null;
    try {
      // A live timer ticket owns refusal backoff. New hand-offs, including duplicates, must
      // not cancel it and advance that backoff merely because the release provider still fails.
      if (armed != null && armed.timer) {
        if (!armed.executor.isShutdown() && timer.get() == armed.executor) return true;
        cancelArmed();
      }
      executor = scheduler.get();
      if (armed != null) {
        boolean stale = armed.executor.isShutdown() || (!armed.timer && armed.executor != executor);
        if (!stale && (armed.timer || armed.due - queue.peek().due <= 0)) return true;
        cancelArmed();
      }
      Ticket ticket = new Ticket(executor, false, queue.peek().due);
      armed = ticket;
      ticket.future =
          executor.schedule(
              () -> tick(ticket),
              Math.max(0, ticket.due - clock.getAsLong()),
              TimeUnit.NANOSECONDS);
      refusalDelay = 0;
      stoppingLogged = false;
      return true;
    } catch (RuntimeException failure) {
      cancelArmed();
      if (ReleaseRetries.isStopping(failure, executor)) {
        drop(dropped, true, failure, initial);
        return false;
      }
      if (timer != null) {
        refusalDelay = refusalDelay == 0 ? firstRefusal : Math.min(maxRefusal, refusalDelay * 2);
        ScheduledExecutorService timerExecutor = null;
        try {
          timerExecutor = timer.get();
          Ticket ticket = new Ticket(timerExecutor, true, clock.getAsLong() + refusalDelay);
          armed = ticket;
          ticket.future =
              timerExecutor.schedule(() -> rearm(ticket), refusalDelay, TimeUnit.NANOSECONDS);
          stoppingLogged = false;
          long now = clock.getAsLong();
          if (!refusalWarned || now - refusalWarnedAt >= ReleaseRetries.WARN_INTERVAL.toNanos()) {
            if (initial != null && initial.retrying != null)
              initial.retrying.accept(refusalDelay, failure);
            else refusal.retrying(refusalDelay, failure);
            refusalWarned = true;
            refusalWarnedAt = now;
          }
          return true;
        } catch (RuntimeException timerFailure) {
          cancelArmed();
          drop(
              dropped,
              ReleaseRetries.isStopping(timerFailure, timerExecutor),
              timerFailure,
              initial);
          return false;
        }
      }
      drop(dropped, false, failure, initial);
      return false;
    }
  }

  private void cancelArmed() {
    if (armed != null && armed.future != null) armed.future.cancel(false);
    armed = null;
  }

  private void drop(
      List<Entry> dropped, boolean stopping, RuntimeException failure, Entry initial) {
    int count = queue.size();
    while (!queue.isEmpty()) {
      Entry entry = queue.remove();
      pending.remove(entry.key, entry);
      dropped.add(entry);
    }
    if (!stopping || !stoppingLogged) {
      if (!stopping && count == 1 && initial != null && initial.refused != null)
        initial.refused.accept(stopping, failure);
      else refusal.dropped(count, stopping, failure);
    }
    if (stopping) stoppingLogged = true;
  }

  private void rearm(Ticket ticket) {
    List<Entry> dropped = new ArrayList<>();
    synchronized (lock) {
      if (armed != ticket) return;
      armed = null;
      arm(dropped);
    }
    notifyDropped(dropped);
  }

  private void tick(Ticket ticket) {
    synchronized (lock) {
      if (armed != ticket || running) return;
      armed = null;
      running = true;
      long now = clock.getAsLong();
      long lag = queue.isEmpty() ? 0 : Math.max(0, now - queue.peek().due);
      if ((pending.size() > 1000 || lag > Duration.ofMinutes(1).toNanos())
          && (!backlogWarned || now - backlogWarnedAt >= ReleaseRetries.WARN_INTERVAL.toNanos())) {
        log.warnf(
            "Backlog of %s: %s pending; oldest entry %s seconds past due",
            kind, pending.size(), TimeUnit.NANOSECONDS.toSeconds(lag));
        backlogWarned = true;
        backlogWarnedAt = now;
      }
    }
    long started = clock.getAsLong();
    try {
      for (int processed = 0; processed < MAX_ITEMS; processed++) {
        Entry entry;
        synchronized (lock) {
          if (queue.isEmpty() || queue.peek().due - clock.getAsLong() > 0) break;
          entry = queue.remove();
        }
        Long next = null;
        try {
          next = entry.attempt.run();
        } catch (Error failure) {
          boolean owned;
          synchronized (lock) {
            owned = pending.remove(entry.key, entry);
          }
          log.errorf(failure, "Error while retrying %s", kind);
          if (owned) notifyDropped(List.of(entry));
          throw failure;
        } finally {
          synchronized (lock) {
            if (next == null) pending.remove(entry.key, entry);
            else if (pending.get(entry.key) == entry) enqueue(entry, next);
          }
        }
        if (clock.getAsLong() - started >= TICK_NANOS) break;
      }
    } finally {
      List<Entry> dropped = new ArrayList<>();
      synchronized (lock) {
        running = false;
        arm(dropped);
      }
      notifyDropped(dropped);
    }
  }

  private void notifyDropped(List<Entry> dropped) {
    for (Entry entry : dropped) {
      try {
        entry.dropped.run();
      } catch (RuntimeException | Error failure) {
        log.errorf(failure, "Dropped callback failed for %s", kind);
      }
    }
  }
}
