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

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import run.ratchet.api.JobContext;

/** Small public targets packaged into the callback integration test deployment. */
public final class RuntimeCallbackJob {
  private static final ConcurrentMap<UUID, JobContext> SUCCESSES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<UUID, Failure> FAILURES = new ConcurrentHashMap<>();

  private RuntimeCallbackJob() {}

  public static void succeed() {}

  public static void fail() {
    throw new IllegalStateException("callback integration failure");
  }

  public static void onSuccess(JobContext context) {
    SUCCESSES.put(context.jobId(), context);
  }

  public static void onFailure(JobContext context, Throwable error) {
    FAILURES.put(context.jobId(), new Failure(context, error));
  }

  public static JobContext success(UUID id) {
    return SUCCESSES.get(id);
  }

  public static Failure failure(UUID id) {
    return FAILURES.get(id);
  }

  public static void reset() {
    SUCCESSES.clear();
    FAILURES.clear();
  }

  public record Failure(JobContext context, Throwable error) {}
}
