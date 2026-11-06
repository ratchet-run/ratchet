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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobContext;

/** Records pages and invocations from real continued executions. */
public class ContinuationJob {
  public static final Map<UUID, List<Integer>> PAGES = new ConcurrentHashMap<>();
  public static final Map<UUID, AtomicInteger> CALLS = new ConcurrentHashMap<>();

  public static void pages() {
    JobContext context = JobContext.current();
    int next = Integer.parseInt(context.lastCheckpoint().orElse("0"));
    PAGES.computeIfAbsent(context.jobId(), id -> new CopyOnWriteArrayList<>()).add(next);
    context.checkpoint(Integer.toString(next + 1));
    if (next < 4) context.requestContinuation();
  }

  public static void always() {
    JobContext context = JobContext.current();
    CALLS.computeIfAbsent(context.jobId(), id -> new AtomicInteger()).incrementAndGet();
    context.requestContinuation();
  }
}
