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
package run.ratchet.store.spi;

/**
 * Lease policy for resource waiters. Slack lets a waiter miss its due time by one poll without
 * losing its place. The cost is that a head waiter no node can claim can keep a free permit idle
 * for at most one lease before younger jobs may pass it.
 */
public final class ResourceWaiterLease {
  /**
   * Slack never drops below the poller's default maximum delay (10 s), so a due waiter is normally
   * re-checked by a polling node before its lease runs out.
   */
  public static final long MIN_SLACK_MS = 10_000;

  private ResourceWaiterLease() {}

  /** Lease = retry delay + slack, where slack is one more retry delay, at least MIN_SLACK_MS. */
  public static long leaseMillis(int retryDelayMs) {
    return retryDelayMs + Math.max(retryDelayMs, MIN_SLACK_MS);
  }
}
