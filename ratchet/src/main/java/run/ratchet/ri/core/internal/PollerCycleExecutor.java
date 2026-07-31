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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Runs poller callbacks without a transaction inherited from the thread that scheduled them.
 *
 * <p>The {@link TxType#NOT_SUPPORTED} boundary is load bearing. A managed scheduled executor can
 * carry the scheduling thread's JTA context into a poll cycle. Suspending it here lets each store's
 * {@code REQUIRED} claim operation start and commit its own transaction before the claimed work is
 * submitted to a managed executor. Otherwise the poll cycle can dispatch before its ambient
 * transaction commits, leaving the worker's separate transaction to read the job's previous {@code
 * PENDING} state.
 */
@ApplicationScoped
@Transactional(TxType.NOT_SUPPORTED)
public class PollerCycleExecutor {

  private final Supplier<Poller> pollerSupplier;

  protected PollerCycleExecutor() {
    this.pollerSupplier = null;
  }

  @Inject
  public PollerCycleExecutor(Poller poller) {
    this(() -> poller);
  }

  /** Creates a cycle executor with lazy poller resolution, breaking the scheduler/poller cycle. */
  public PollerCycleExecutor(Supplier<Poller> pollerSupplier) {
    this.pollerSupplier = Objects.requireNonNull(pollerSupplier, "pollerSupplier must not be null");
  }

  public long tick() {
    return poller().tick();
  }

  public void onWakeup() {
    poller().onWakeup();
  }

  private Poller poller() {
    return Objects.requireNonNull(pollerSupplier.get(), "pollerSupplier returned null");
  }
}
