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
package run.ratchet.micronaut.internal;

import io.micronaut.context.BeanContext;
import io.micronaut.context.env.Environment;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionOperationsRegistry;
import jakarta.inject.Singleton;

/** Shared datasource selection for store, engine boundaries and after-commit registration. */
@Singleton
public class RatchetTransactionOperations {
  private final BeanContext context;
  private final TransactionOperationsRegistry registry;
  private final String datasourceName;
  private volatile TransactionOperations<?> operations;

  public RatchetTransactionOperations(
      BeanContext context, Environment environment, TransactionOperationsRegistry registry) {
    this.context = context;
    this.registry = registry;
    datasourceName =
        environment.getProperty("ratchet.jpa.datasource", String.class).orElse("default");
    if (datasourceName.isBlank()) {
      throw new IllegalArgumentException("ratchet.jpa.datasource must name a datasource");
    }
  }

  public String datasourceName() {
    return datasourceName;
  }

  public <T> T selected(Class<T> type) {
    return context
        .findBean(type, Qualifiers.byName(datasourceName))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Ratchet requires "
                        + type.getSimpleName()
                        + " for datasource '"
                        + datasourceName
                        + "'"));
  }

  public TransactionOperations<?> operations() {
    TransactionOperations<?> resolved = operations;
    if (resolved == null) {
      synchronized (this) {
        resolved = operations;
        if (resolved == null) {
          resolved = resolveOperations();
          operations = resolved;
        }
      }
    }
    return resolved;
  }

  private TransactionOperations<?> resolveOperations() {
    NoTransactionOperations inert = context.findBean(NoTransactionOperations.class).orElse(null);
    if (inert != null) return inert;
    return context
        .findBean(RatchetTransactionManagerProvider.class)
        .map(RatchetTransactionManagerProvider::transactions)
        .orElseGet(() -> registry.provideSynchronous(TransactionOperations.class, datasourceName));
  }
}
