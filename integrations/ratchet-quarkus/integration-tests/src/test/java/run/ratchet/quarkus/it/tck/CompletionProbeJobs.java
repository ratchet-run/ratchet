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

import java.util.concurrent.atomic.AtomicInteger;

public final class CompletionProbeJobs {
  public static final AtomicInteger roots = new AtomicInteger();
  public static final AtomicInteger children = new AtomicInteger();
  public static final AtomicInteger callbacks = new AtomicInteger();

  public static void root() {
    roots.incrementAndGet();
  }

  public static void child() {
    children.incrementAndGet();
  }

  public static void item(String item) {
    children.incrementAndGet();
  }

  public static void callback() {
    callbacks.incrementAndGet();
  }

  public static void reset() {
    roots.set(0);
    children.set(0);
    callbacks.set(0);
  }
}
