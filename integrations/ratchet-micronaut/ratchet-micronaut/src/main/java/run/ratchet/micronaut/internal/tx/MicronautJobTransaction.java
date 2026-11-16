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
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import run.ratchet.micronaut.internal.MicronautTransactions;
import run.ratchet.ri.core.JobTransaction;

/** Micronaut carrier for the job-body transaction boundary. */
public class MicronautJobTransaction extends JobTransaction {
  private final Supplier<? extends TransactionOperations<?>> operations;

  // The boundary proxy is built through this constructor and delegates to the factory's instance.
  @Creator
  public MicronautJobTransaction() {
    this(() -> null);
  }

  public MicronautJobTransaction(Supplier<? extends TransactionOperations<?>> operations) {
    this.operations = operations;
  }

  /**
   * Detects transactions only on the manager selected by ratchet.jpa.datasource. A transaction on
   * another datasource is not detected.
   */
  @Override
  public boolean hasActiveTransaction() {
    TransactionOperations<?> manager = operations.get();
    return manager != null && MicronautTransactions.hasActiveTransaction(manager);
  }

  @Override
  @Transactional(value = TxType.REQUIRED, rollbackOn = Exception.class)
  public <T> T run(Callable<T> work) throws Exception {
    return insideCarrier(work);
  }
}
