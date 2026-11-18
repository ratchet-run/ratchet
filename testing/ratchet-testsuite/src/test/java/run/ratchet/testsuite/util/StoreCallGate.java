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
package run.ratchet.testsuite.util;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test hook that pauses or watches chosen calls on the deployed store bean. {@link
 * StoreCallGateDecorator} feeds it, and a test arms it in-container. Nothing ships in a production
 * module.
 */
public final class StoreCallGate {
  private static final CopyOnWriteArrayList<Pause> PAUSES = new CopyOnWriteArrayList<>();
  private static final CopyOnWriteArrayList<Watch> WATCHES = new CopyOnWriteArrayList<>();
  private static final ConcurrentHashMap<UUID, List<String>> CALLS = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<UUID, List<ReturnedCall>> RETURNED_CALLS =
      new ConcurrentHashMap<>();
  private static final Object RETURNS_LOCK = new Object();
  // StoreCallGateDecoratorTest keeps this set aligned with the decorator overrides.
  private static final Set<String> GATED_METHODS =
      Set.of(
          "findById",
          "getJobStatus",
          "incrementRetryAttempt",
          "scheduleJobRetry",
          "releaseBusinessKeyHold");
  private static final Duration MAX_PAUSE = Duration.ofSeconds(60);

  private StoreCallGate() {}

  /** Returns the names of the store methods covered by the gate. */
  public static Set<String> gatedMethods() {
    return GATED_METHODS;
  }

  /**
   * Pauses the first later call of {@code method} for this job made from inside {@code caller}
   * (whose class name must appear on the calling thread's stack), on any thread except the one
   * arming it. Later matching calls pass through.
   */
  public static Pause pause(String method, UUID jobId, Class<?> caller) {
    requireCovered(method);
    Pause pause = new Pause(method, jobId, caller, null);
    PAUSES.add(pause);
    return pause;
  }

  /**
   * Pauses the first later call of {@code method} from inside {@code caller} for any job id, except
   * on the arming thread or for a null id. Use only while one job can reach that call. The test
   * must check {@link Pause#jobId()} after it fires.
   */
  public static Pause pauseNext(String method, Class<?> caller) {
    return pause(method, null, caller);
  }

  /**
   * Pauses the first later call from the chosen caller method. The same restrictions as {@link
   * #pauseNext(String, Class)} apply.
   */
  public static Pause pauseNext(String method, Class<?> caller, String callerMethod) {
    requireCovered(method);
    Pause pause = new Pause(method, null, caller, callerMethod);
    PAUSES.add(pause);
    return pause;
  }

  /**
   * Counts calls of {@code method} for this job that returned or threw since the last reset,
   * including calls completed before this watch was created.
   */
  public static Watch watch(String method, UUID jobId) {
    requireCovered(method);
    Watch watch = new Watch(method, jobId);
    synchronized (RETURNS_LOCK) {
      List<ReturnedCall> returned = RETURNED_CALLS.get(jobId);
      if (returned != null) {
        watch.count.set(
            (int) returned.stream().filter(call -> call.method().equals(method)).count());
      }
      WATCHES.add(watch);
    }
    return watch;
  }

  /** The names of the store methods called for this job so far, in the order they were entered. */
  public static List<String> calls(UUID jobId) {
    List<String> calls = CALLS.get(jobId);
    return calls == null ? List.of() : List.copyOf(calls);
  }

  /** Returns whether the covered store method was called for this job. */
  public static boolean called(UUID jobId, String method) {
    requireCovered(method);
    return calls(jobId).contains(method);
  }

  /** Returns whether this method returned the given result on this thread since the last reset. */
  public static boolean returnedOn(UUID jobId, String method, Thread thread, Object result) {
    requireCovered(method);
    synchronized (RETURNS_LOCK) {
      List<ReturnedCall> returned = RETURNED_CALLS.get(jobId);
      return returned != null
          && returned.stream()
              .anyMatch(
                  call ->
                      call.method().equals(method)
                          && call.thread() == thread
                          && call.completed()
                          && Objects.equals(call.result(), result));
    }
  }

  /** Returns whether this method returned or threw on this thread since the last reset. */
  public static boolean calledOn(UUID jobId, String method, Thread thread) {
    requireCovered(method);
    synchronized (RETURNS_LOCK) {
      List<ReturnedCall> returned = RETURNED_CALLS.get(jobId);
      return returned != null
          && returned.stream()
              .anyMatch(call -> call.method().equals(method) && call.thread() == thread);
    }
  }

  private record ReturnedCall(String method, Thread thread, boolean completed, Object result) {}

  private static void requireCovered(String method) {
    if (!GATED_METHODS.contains(method)) {
      throw new IllegalArgumentException(
          "Uncovered store method " + method + "; covered methods: " + GATED_METHODS);
    }
  }

  /**
   * Releases every pause, waits up to {@code awaitReturn} for paused calls to return, then forgets
   * all pauses, watches and recorded calls. It waits only for each paused store call to return, not
   * for the engine to finish the rest of that operation. A passing test has already waited for the
   * outcome it checks. For {@code @AfterEach}, so no engine thread stays parked inside a
   * transaction when the test ends.
   */
  public static void reset(Duration awaitReturn) {
    PAUSES.forEach(
        pause -> {
          pause.state.compareAndSet(Pause.State.ARMED, Pause.State.RETIRED);
          pause.release();
        });
    long deadline = System.nanoTime() + awaitReturn.toNanos();
    try {
      for (Pause pause : PAUSES) {
        if (pause.state.get() == Pause.State.FIRED) {
          if (!pause.waitForReturn(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())))) {
            throw new AssertionError(
                "Paused "
                    + pause.method
                    + " call for job "
                    + pause.jobId
                    + " did not return within "
                    + awaitReturn);
          }
        }
        if (pause.expired) {
          throw new AssertionError(
              "Paused " + pause.method + " call for job " + pause.jobId + " expired");
        }
      }
    } finally {
      PAUSES.clear();
      synchronized (RETURNS_LOCK) {
        WATCHES.clear();
        RETURNED_CALLS.clear();
      }
      CALLS.clear();
    }
  }

  /**
   * Records the call and parks the thread when an armed pause matches. Returns that pause or null.
   */
  static Pause enter(String method, UUID jobId) {
    // Let a call without a job id reach the store unchanged.
    if (jobId == null) return null;
    CALLS.computeIfAbsent(jobId, ignored -> new CopyOnWriteArrayList<>()).add(method);
    for (Pause pause : PAUSES) {
      if (!pause.method.equals(method)
          || (pause.targetJobId != null && !pause.targetJobId.equals(jobId))
          || Thread.currentThread() == pause.armingThread) {
        continue;
      }
      boolean fromCaller =
          StackWalker.getInstance()
              .walk(
                  frames ->
                      frames.anyMatch(
                          frame ->
                              frame.getClassName().equals(pause.callerClassName)
                                  && (pause.callerMethod == null
                                      || frame.getMethodName().equals(pause.callerMethod))));
      if (fromCaller && pause.state.compareAndSet(Pause.State.ARMED, Pause.State.FIRED)) {
        pause.jobId = jobId;
        pause.thread = Thread.currentThread();
        pause.parked.countDown();
        long deadline = System.nanoTime() + MAX_PAUSE.toNanos();
        boolean interrupted = false;
        boolean wasReleased = false;
        try {
          long remaining;
          while ((remaining = deadline - System.nanoTime()) > 0) {
            try {
              if (pause.released.await(remaining, TimeUnit.NANOSECONDS)) {
                wasReleased = true;
                break;
              }
              break;
            } catch (InterruptedException ex) {
              interrupted = true;
            }
          }
          if (!wasReleased) {
            pause.expired = true;
          }
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
        return pause;
      }
    }
    return null;
  }

  /** Counts matching watches and marks a fired pause as returned. */
  static void exit(String method, UUID jobId, Pause fired, boolean completed, Object result) {
    if (jobId == null) return;
    synchronized (RETURNS_LOCK) {
      RETURNED_CALLS
          .computeIfAbsent(jobId, ignored -> new CopyOnWriteArrayList<>())
          .add(new ReturnedCall(method, Thread.currentThread(), completed, result));
      for (Watch watch : WATCHES) {
        if (watch.method.equals(method) && watch.jobId.equals(jobId)) {
          synchronized (watch) {
            watch.count.incrementAndGet();
            watch.notifyAll();
          }
        }
      }
    }
    if (fired != null) {
      fired.returned.countDown();
    }
  }

  /** A single store call that can be held until the test releases it. */
  public static final class Pause {
    private final String method;
    private final UUID targetJobId;
    private volatile UUID jobId;
    private volatile Thread thread;
    private final String callerClassName;
    private final String callerSimpleName;
    private final String callerMethod;
    private final Thread armingThread;

    private enum State {
      ARMED,
      FIRED,
      RETIRED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.ARMED);
    private volatile boolean expired;
    private final CountDownLatch parked = new CountDownLatch(1);
    private final CountDownLatch released = new CountDownLatch(1);
    private final CountDownLatch returned = new CountDownLatch(1);

    private Pause(String method, UUID jobId, Class<?> caller, String callerMethod) {
      this.method = method;
      this.targetJobId = jobId;
      this.callerClassName = caller.getName();
      this.callerSimpleName = caller.getSimpleName();
      this.callerMethod = callerMethod;
      armingThread = Thread.currentThread();
    }

    /** Returns the job id of the paused call, or null before it fires. */
    public UUID jobId() {
      return jobId;
    }

    /** Returns the thread of the paused call, or null before it fires. */
    public Thread thread() {
      return thread;
    }

    /** Waits until the chosen store call reaches the gate. */
    public void awaitParked(Duration timeout) {
      try {
        if (!parked.await(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
          throw new AssertionError(
              "No "
                  + method
                  + " call for job "
                  + jobId
                  + " from "
                  + callerSimpleName
                  + (callerMethod == null ? "" : "." + callerMethod)
                  + " reached the gate within "
                  + timeout);
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AssertionError(ex);
      }
    }

    /** Lets the parked call proceed. */
    public void release() {
      released.countDown();
    }

    /** Waits for the paused store call to return or throw. */
    public void awaitReturned(Duration timeout) {
      if (state.get() != State.FIRED) {
        throw new AssertionError(
            "Pause for " + method + " call for job " + jobId + " has not fired");
      }
      if (!waitForReturn(timeout)) {
        throw new AssertionError(
            "Paused " + method + " call for job " + jobId + " did not return within " + timeout);
      }
    }

    private boolean waitForReturn(Duration timeout) {
      try {
        return state.get() != State.FIRED
            || returned.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new AssertionError(ex);
      }
    }
  }

  /** Counts completed calls of a chosen store method for one job. */
  public static final class Watch {
    private final String method;
    private final UUID jobId;
    private final AtomicInteger count = new AtomicInteger();

    private Watch(String method, UUID jobId) {
      this.method = method;
      this.jobId = jobId;
    }

    /** Waits for at least {@code expected} calls to return or throw. */
    public synchronized void awaitCalls(int expected, Duration timeout) {
      long deadline = System.nanoTime() + timeout.toNanos();
      while (count.get() < expected) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          throw new AssertionError(
              "Expected "
                  + expected
                  + " "
                  + method
                  + " call(s) for job "
                  + jobId
                  + " within "
                  + timeout
                  + ", saw "
                  + count.get());
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(this, remaining);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new AssertionError(ex);
        }
      }
    }

    /** Returns the number of calls that have returned or thrown. */
    public int count() {
      return count.get();
    }
  }
}
