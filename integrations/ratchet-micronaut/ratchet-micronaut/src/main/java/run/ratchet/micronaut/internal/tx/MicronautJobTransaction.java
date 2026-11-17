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
import io.micronaut.transaction.TransactionOperations;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.ri.core.JobTransaction;

/** Micronaut carrier that detects open transactions on every datasource manager. */
public class MicronautJobTransaction extends JobTransaction {
  private final Supplier<? extends Iterable<? extends TransactionOperations<?>>> operations;

  // The boundary proxy is built through this constructor and delegates to the factory's instance.
  @Creator
  public MicronautJobTransaction() {
    this(() -> List.of());
  }

  public MicronautJobTransaction(
      Supplier<? extends Iterable<? extends TransactionOperations<?>>> operations) {
    this.operations = operations;
  }

  /** Detects transactions on any manager in the context. */
  @Override
  public boolean hasActiveTransaction() {
    for (TransactionOperations<?> manager : operations.get()) {
      if (MicronautTransactions.hasActiveTransaction(manager)) return true;
    }
    return false;
  }

  @Override
  @Transactional(value = TxType.REQUIRED, rollbackOn = Exception.class)
  public <T> T run(Callable<T> work) throws Exception {
    return insideCarrier(work);
  }
}
