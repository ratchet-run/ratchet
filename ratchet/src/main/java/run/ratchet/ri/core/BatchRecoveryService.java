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
package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import run.ratchet.store.spi.BatchStore;

/**
 * Transaction boundary for recovering a single completed batch.
 *
 * <p>The completion callback deliberately remains owned by {@link BatchService}; this bean exists
 * only to guarantee that recovery crosses a container-advised {@code REQUIRES_NEW} boundary.
 */
@ApplicationScoped
public class BatchRecoveryService {

  private final BatchStore batchStore;

  protected BatchRecoveryService() {
    this.batchStore = null;
  }

  /** Portable constructor for containers that resolve optional store capabilities themselves. */
  public BatchRecoveryService(BatchStore batchStore) {
    this.batchStore = batchStore;
  }

  @Inject
  BatchRecoveryService(Instance<BatchStore> batchStore) {
    this(batchStore.isResolvable() ? batchStore.get() : null);
  }

  /**
   * Atomically claims a recoverable batch and runs its shared completion algorithm in a new
   * transaction.
   *
   * @param batchId batch parent identifier
   * @param completion shared non-boundary completion logic supplied by {@link BatchService}
   * @return whether recovery performed an observable completion transition
   */
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public boolean recoverCompletedBatch(UUID batchId, BooleanSupplier completion) {
    if (batchStore == null || !batchStore.markBatchCompleteIfReady(batchId)) {
      return false;
    }
    return completion.getAsBoolean();
  }
}
