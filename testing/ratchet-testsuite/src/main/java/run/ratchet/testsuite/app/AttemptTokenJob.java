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
package run.ratchet.testsuite.app;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import run.ratchet.api.AttemptToken;
import run.ratchet.api.JobContext;

/** Keeps the actual contexts so assertions can query an attempt from another thread. */
public class AttemptTokenJob {
  public record Attempt(JobContext context, AttemptToken token, CountDownLatch release) {}

  private static final Map<UUID, List<Attempt>> ATTEMPTS = new ConcurrentHashMap<>();
  private static final List<Attempt> BLOCKED = new CopyOnWriteArrayList<>();
  // Recurring firings go to their own list, and only while a test watches them, so a firing that
  // starts after its test cannot block or show up in another test.
  private static final List<Attempt> FIRINGS = new CopyOnWriteArrayList<>();
  private static final AtomicBoolean FIRINGS_OPEN = new AtomicBoolean();
  public static final AtomicReference<JobContext> FAILURE_CONTEXT = new AtomicReference<>();
  public static final AtomicReference<Optional<AttemptToken>> FAILURE_TOKEN =
      new AtomicReference<>();
  public static final AtomicReference<Boolean> FAILURE_CURRENT = new AtomicReference<>();

  public static void retryAndBlock() {
    execute(true, true);
  }

  public static void block() {
    execute(false, true);
  }

  public static void succeed() {
    execute(false, false);
  }

  public static void fail() {
    execute(true, false);
  }

  public static void execute(boolean failFirst, boolean block) {
    JobContext context = JobContext.current();
    var attempt = new Attempt(context, context.attemptToken().orElseThrow(), new CountDownLatch(1));
    var attempts = ATTEMPTS.computeIfAbsent(context.jobId(), id -> new CopyOnWriteArrayList<>());
    attempts.add(attempt);
    if (failFirst && attempts.size() == 1) {
      throw new IllegalStateException("First attempt fails");
    }
    if (block) {
      BLOCKED.add(attempt);
      awaitRelease(attempt);
    }
  }

  /** Blocks a recurring firing while a test watches firings; any other firing returns at once. */
  public static void blockFiring() {
    JobContext context = JobContext.current();
    var attempt = new Attempt(context, context.attemptToken().orElseThrow(), new CountDownLatch(1));
    if (!FIRINGS_OPEN.get()) {
      return;
    }
    FIRINGS.add(attempt);
    awaitRelease(attempt);
  }

  private static void awaitRelease(Attempt attempt) {
    try {
      if (!attempt.release().await(60, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Attempt was not released");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while blocked", e);
    }
  }

  public static void openFirings() {
    FIRINGS_OPEN.set(true);
  }

  public static List<Attempt> firings() {
    return List.copyOf(FIRINGS);
  }

  public static void batchItem(String item) {
    execute(false, true);
  }

  public static List<Attempt> attempts(UUID id) {
    return List.copyOf(ATTEMPTS.getOrDefault(id, List.of()));
  }

  public static List<Attempt> blocked() {
    return List.copyOf(BLOCKED);
  }

  public static void recordFailureCallback() {
    JobContext context = JobContext.current();
    FAILURE_TOKEN.set(context.attemptToken());
    FAILURE_CURRENT.set(context.isCurrentAttempt());
    FAILURE_CONTEXT.set(context);
  }

  public static void releaseAll() {
    ATTEMPTS.values().forEach(list -> list.forEach(attempt -> attempt.release().countDown()));
    FIRINGS.forEach(attempt -> attempt.release().countDown());
  }

  public static void reset() {
    FIRINGS_OPEN.set(false);
    releaseAll();
    ATTEMPTS.clear();
    BLOCKED.clear();
    FIRINGS.clear();
    FAILURE_CONTEXT.set(null);
    FAILURE_TOKEN.set(null);
    FAILURE_CURRENT.set(null);
  }
}
