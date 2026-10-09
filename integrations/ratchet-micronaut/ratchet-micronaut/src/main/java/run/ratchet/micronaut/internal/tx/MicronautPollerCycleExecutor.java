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
package run.ratchet.micronaut.internal.tx;

import io.micronaut.core.annotation.Creator;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.util.function.Supplier;
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerCycleExecutor;

/**
 * Micronaut proxy carrier for {@link PollerCycleExecutor}; see {@link RatchetTransactionBoundary}
 * and the package documentation for why.
 */
@Transactional(TxType.NOT_SUPPORTED)
public class MicronautPollerCycleExecutor extends PollerCycleExecutor {
  @Creator
  public MicronautPollerCycleExecutor() {
    super();
  }

  public MicronautPollerCycleExecutor(Poller poller) {
    super(poller);
  }

  private Supplier<Poller> pollerProvider;

  /** Resolves the poller only when a callback runs, after the bean graph is constructed. */
  public MicronautPollerCycleExecutor(Supplier<Poller> pollerProvider) {
    super();
    this.pollerProvider = pollerProvider;
  }

  @Override
  public long tick() {
    return pollerProvider == null ? super.tick() : pollerProvider.get().tick();
  }

  @Override
  public void onWakeup() {
    if (pollerProvider == null) {
      super.onWakeup();
    } else {
      pollerProvider.get().onWakeup();
    }
  }
}
