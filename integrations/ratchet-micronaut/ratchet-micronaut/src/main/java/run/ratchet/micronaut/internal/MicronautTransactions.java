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

import io.micronaut.transaction.TransactionCallback;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionStatus;
import io.micronaut.transaction.exceptions.IllegalTransactionStateException;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** JTA propagation and rollback rules over Micronaut's propagated transaction scopes. */
public final class MicronautTransactions {
  // Private status kinds of Micronaut Data 4.14 and 5.2. A rename fails fast below rather than
  // silently joining an empty scope.
  private static final String EXISTING =
      "io.micronaut.transaction.impl.DefaultTransactionStatus$ExistingTransactionStatus";
  private static final String NO_TX =
      "io.micronaut.transaction.impl.DefaultTransactionStatus$NoTxTransactionStatus";
  private static volatile Field existingTransaction;

  private MicronautTransactions() {}

  private static synchronized Field existingTransactionField() {
    if (existingTransaction != null) return existingTransaction;
    try {
      Field field = Class.forName(EXISTING).getDeclaredField("existingTransaction");
      if (!field.trySetAccessible()) throw new IllegalAccessException("existingTransaction");
      Class.forName(NO_TX);
      existingTransaction = field;
      return field;
    } catch (ReflectiveOperationException | RuntimeException failure) {
      throw new IllegalStateException(
          "Unsupported Micronaut Data version: transaction status internals changed", failure);
    }
  }

  public static TransactionStatus<?> rootStatus(TransactionStatus<?> status) {
    while (status.getClass().getName().equals(EXISTING)) {
      try {
        status = (TransactionStatus<?>) existingTransactionField().get(status);
      } catch (IllegalAccessException failure) {
        throw new IllegalStateException(
            "Unsupported Micronaut Data version: cannot unwrap transaction status", failure);
      }
    }
    return status;
  }

  public static boolean hasActiveTransaction(TransactionOperations<?> operations) {
    existingTransactionField();
    TransactionStatus<?> status = operations.findTransactionStatus().orElse(null);
    if (status == null || status.isCompleted()) return false;
    TransactionStatus<?> root = rootStatus(status);
    return !root.isCompleted() && !root.getClass().getName().equals(NO_TX);
  }

  public static Propagation effectivePropagation(TransactionOperations<?> operations, TxType type) {
    // Without a transaction manager every boundary is inert, MANDATORY included, as unproxied
    // Spring beans are; the fallback never reports an active transaction.
    if (operations instanceof NoTransactionOperations) return propagation(type);
    if (type == TxType.REQUIRED || type == TxType.MANDATORY) {
      if (!hasActiveTransaction(operations)) {
        if (type == TxType.MANDATORY) {
          throw new IllegalTransactionStateException(
              "No existing transaction found for transaction marked with propagation 'mandatory'");
        }
        return Propagation.REQUIRES_NEW;
      }
    }
    return propagation(type);
  }

  public static <C, R> R execute(
      TransactionOperations<C> operations, TxType type, TransactionCallback<C, R> callback) {
    return operations.execute(
        TransactionDefinition.of(effectivePropagation(operations, type)), callback);
  }

  public static TransactionDefinition definition(
      TransactionOperations<?> operations, Transactional annotation) {
    Propagation effective = effectivePropagation(operations, annotation.value());
    return definition(annotation, effective);
  }

  public static Propagation propagation(TxType type) {
    return switch (type) {
      case REQUIRED -> Propagation.REQUIRED;
      case REQUIRES_NEW -> Propagation.REQUIRES_NEW;
      case MANDATORY -> Propagation.MANDATORY;
      case SUPPORTS -> Propagation.SUPPORTS;
      case NOT_SUPPORTED -> Propagation.NOT_SUPPORTED;
      case NEVER -> Propagation.NEVER;
    };
  }

  public static TransactionDefinition definition(Transactional annotation) {
    return definition(annotation, propagation(annotation.value()));
  }

  private static TransactionDefinition definition(Transactional annotation, Propagation effective) {
    return new TransactionDefinition() {
      @Override
      public Propagation getPropagationBehavior() {
        return effective;
      }

      @Override
      public Collection<Class<? extends Throwable>> getRollbackOn() {
        return throwableTypes(annotation.rollbackOn());
      }

      @Override
      public Collection<Class<? extends Throwable>> getDontRollbackOn() {
        return throwableTypes(annotation.dontRollbackOn());
      }

      @Override
      public boolean rollbackOn(Throwable failure) {
        // JTA gives dontRollbackOn precedence, irrespective of inheritance distance.
        for (Class<?> type : annotation.dontRollbackOn())
          if (type.isInstance(failure)) return false;
        for (Class<?> type : annotation.rollbackOn()) if (type.isInstance(failure)) return true;
        return failure instanceof RuntimeException || failure instanceof Error;
      }
    };
  }

  private static List<Class<? extends Throwable>> throwableTypes(Class<?>[] types) {
    List<Class<? extends Throwable>> result = new ArrayList<>();
    for (Class<?> type : types) result.add(type.asSubclass(Throwable.class));
    return List.copyOf(result);
  }
}
