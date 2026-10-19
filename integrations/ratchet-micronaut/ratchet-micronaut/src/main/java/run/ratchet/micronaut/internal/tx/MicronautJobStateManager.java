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
import run.ratchet.micronaut.internal.RatchetTransactionBoundary;
import run.ratchet.ri.core.JobStateManager;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobBatchStatusStore;

/**
 * Micronaut proxy carrier for {@link JobStateManager}; see {@link RatchetTransactionBoundary} and
 * the package documentation for why.
 */
@Transactional
public class MicronautJobStateManager extends JobStateManager {
  @Creator
  public MicronautJobStateManager() {
    super();
  }

  public MicronautJobStateManager(
      JobBatchStatusStore jobBatchStatusStore, NodeIdentityProvider nodeIdentityProvider) {
    super(jobBatchStatusStore, nodeIdentityProvider);
  }
}
