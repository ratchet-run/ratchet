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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import run.ratchet.api.JobContext;

/** Progress reports from real scheduler attempts. */
public class ProgressJob {
  public static volatile CountDownLatch reported = new CountDownLatch(1);
  public static volatile CountDownLatch release = new CountDownLatch(1);

  public static void reset() {
    reported = new CountDownLatch(1);
    release = new CountDownLatch(1);
  }

  public static void live() throws InterruptedException {
    JobContext context = JobContext.current();
    context.progress(3, 10);
    reported.countDown();
    if (!release.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Job not released");
    context.progress(10, 10);
  }

  public static void immediate() {
    JobContext.current().progress(5, 5);
  }
}
