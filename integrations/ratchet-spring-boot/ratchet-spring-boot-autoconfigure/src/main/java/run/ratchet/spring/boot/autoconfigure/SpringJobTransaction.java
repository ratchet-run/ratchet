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
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.ri.core.JobTransaction;

/** Explicit transaction template; excluded from automatic transaction proxying. */
public class SpringJobTransaction extends JobTransaction implements AopInfrastructureBean {
  private final Supplier<PlatformTransactionManager> manager;

  public SpringJobTransaction(PlatformTransactionManager manager) {
    this(() -> manager);
  }

  public SpringJobTransaction(Supplier<PlatformTransactionManager> manager) {
    this.manager = manager;
  }

  @Override
  public <T> T run(Callable<T> work) throws Exception {
    TransactionTemplate transactions = new TransactionTemplate(manager.get());
    transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    try {
      return transactions.execute(
          status -> {
            try {
              return insideCarrier(work);
            } catch (Exception failure) {
              throw new WorkFailure(failure);
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
