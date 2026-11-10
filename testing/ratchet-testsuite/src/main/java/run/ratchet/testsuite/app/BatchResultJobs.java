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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Partial results, reducers and callback observations for BatchResultIT. */
public final class BatchResultJobs {
  private static volatile Object received;
  private static final AtomicInteger successes = new AtomicInteger();
  private static final AtomicInteger failures = new AtomicInteger();
  private static final AtomicInteger folds = new AtomicInteger();

  private BatchResultJobs() {}

  public static void reset() {
    received = null;
    successes.set(0);
    failures.set(0);
    folds.set(0);
  }

  public static Object received() {
    return received;
  }

  public static int successes() {
    return successes.get();
  }

  public static int failures() {
    return failures.get();
  }

  public static int folds() {
    return folds.get();
  }

  public static Long partial(Integer item) {
    return item.longValue();
  }

  public static Long nothing(Integer item) {
    return null;
  }

  public static Long fail(Integer item) {
    throw new IllegalStateException("child failed");
  }

  public static Long gated(Integer item) {
    BatchControlGate.process(item);
    return item.longValue();
  }

  public static Long sum(Long a, Long b) {
    folds.incrementAndGet();
    return a + b;
  }

  public static Long broken(Long a, Long b) {
    folds.incrementAndGet();
    throw new IllegalStateException("combiner failed");
  }

  public static void receive(Long value) {
    received = value;
    successes.incrementAndGet();
  }

  public static void failure() {
    failures.incrementAndGet();
  }

  public static Map<String, Long> word(String item) {
    return Map.of(item, 1L);
  }

  public static Map<String, Long> merge(Map<String, Long> a, Map<String, Long> b) {
    Map<String, Long> result = new HashMap<>(a);
    b.forEach((word, count) -> result.merge(word, count, Long::sum));
    return result;
  }

  public static void receiveWords(Map<String, Long> value) {
    received = value;
    successes.incrementAndGet();
  }

  public static Set<String> singleton(String item) {
    return Set.of(item);
  }

  public static <T> Set<T> union(Set<T> a, Set<T> b) {
    Set<T> result = new HashSet<>(a);
    result.addAll(b);
    return result;
  }

  public static void receiveSet(Set<String> value) {
    received = value;
    successes.incrementAndGet();
  }
}
