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
package run.ratchet.spring.boot.autoconfigure;

import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.ri.core.JobTransaction;

/** Explicit transaction template; excluded from automatic transaction proxying. */
public class SpringJobTransaction extends JobTransaction implements AopInfrastructureBean {
  private final Supplier<PlatformTransactionManager> manager;
  private final ThreadLocal<TransactionStatus> currentStatus = new ThreadLocal<>();

  public SpringJobTransaction(PlatformTransactionManager manager) {
    this(() -> manager);
  }

  public SpringJobTransaction(Supplier<PlatformTransactionManager> manager) {
    this.manager = manager;
  }

  /**
   * Detects transactions on any Spring manager bound to this thread. Rejecting another manager's
   * transaction is intentional: a unit would otherwise hold a second connection.
   */
  @Override
  public boolean hasActiveTransaction() {
    return TransactionSynchronizationManager.isActualTransactionActive();
  }

  @Override
  public boolean isRollbackOnly() {
    TransactionStatus status = currentStatus.get();
    return status != null && status.isRollbackOnly();
  }

  @Override
  public <T> T run(Callable<T> work) throws Exception {
    TransactionTemplate transactions = new TransactionTemplate(manager.get());
    transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    try {
      return transactions.execute(
          status -> {
            TransactionStatus previous = currentStatus.get();
            currentStatus.set(status);
            try {
              return insideCarrier(work);
            } catch (Exception failure) {
              throw new WorkFailure(failure);
            } finally {
              if (previous == null) currentStatus.remove();
              else currentStatus.set(previous);
            }
          });
    } catch (WorkFailure failure) {
      throw (Exception) failure.getCause();
    }
  }

  private static final class WorkFailure extends RuntimeException {
    WorkFailure(Exception cause) {
      super(cause);
    }
  }
}
