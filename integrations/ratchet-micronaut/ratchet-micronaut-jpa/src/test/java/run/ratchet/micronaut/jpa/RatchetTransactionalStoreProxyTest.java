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
package run.ratchet.micronaut.jpa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.micronaut.transaction.TransactionCallback;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.TransactionDefinition.Propagation;
import io.micronaut.transaction.TransactionOperations;
import io.micronaut.transaction.TransactionStatus;
import io.micronaut.transaction.exceptions.IllegalTransactionStateException;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.RatchetTransientStoreException;

class RatchetTransactionalStoreProxyTest {
  @Test
  void mapsEveryJtaPropagation() {
    assertEquals(Propagation.REQUIRED, RatchetTransactionalStoreProxy.propagation(TxType.REQUIRED));
    assertEquals(
        Propagation.REQUIRES_NEW, RatchetTransactionalStoreProxy.propagation(TxType.REQUIRES_NEW));
    assertEquals(
        Propagation.MANDATORY, RatchetTransactionalStoreProxy.propagation(TxType.MANDATORY));
    assertEquals(Propagation.SUPPORTS, RatchetTransactionalStoreProxy.propagation(TxType.SUPPORTS));
    assertEquals(
        Propagation.NOT_SUPPORTED,
        RatchetTransactionalStoreProxy.propagation(TxType.NOT_SUPPORTED));
    assertEquals(Propagation.NEVER, RatchetTransactionalStoreProxy.propagation(TxType.NEVER));
  }

  @Test
  void jtaRollbackRulesKeepDontRollbackOnPrecedenceAndCheckedDefaults() throws Exception {
    var defaults = definition("required");
    assertTrue(defaults.rollbackOn(new IllegalStateException()));
    assertTrue(defaults.rollbackOn(new AssertionError()));
    assertFalse(defaults.rollbackOn(new IOException()));
    var listed = definition("checked");
    assertTrue(listed.rollbackOn(new IOException()));
    assertFalse(listed.rollbackOn(new Exception()));
    var excluded = definition("excluded");
    assertFalse(excluded.rollbackOn(new IllegalArgumentException()));
    assertTrue(excluded.rollbackOn(new IllegalStateException()));
    // Even a broad exclusion wins over a narrower rollback rule (JTA, not closest-match rules).
    assertFalse(definition("overlap").rollbackOn(new IOException()));
  }

  @Test
  void readsImplementationAnnotationsAndClassDefaultUsingMockTransactionOperations()
      throws Exception {
    List<TransactionDefinition> calls = new ArrayList<>();
    TransactionOperations<Object> transactions = transactions(calls);
    TestStore proxy =
        (TestStore)
            RatchetTransactionalStoreProxy.createProxy(
                new AnnotatedStore(), TestStore.class, transactions);
    proxy.required();
    proxy.requiresNew();
    proxy.mandatory();
    proxy.supports();
    proxy.notSupported();
    proxy.never();
    assertEquals(
        List.of(
            Propagation.REQUIRED,
            Propagation.REQUIRES_NEW,
            Propagation.MANDATORY,
            Propagation.SUPPORTS,
            Propagation.NOT_SUPPORTED,
            Propagation.NEVER),
        calls.stream().map(TransactionDefinition::getPropagationBehavior).toList());
    IOException checked = assertThrows(IOException.class, proxy::checked);
    assertEquals("checked", checked.getMessage());
    assertTrue(calls.get(6).rollbackOn(checked));
    assertThrows(IllegalArgumentException.class, proxy::excluded);
    assertFalse(calls.get(7).rollbackOn(new IllegalArgumentException()));
  }

  @Test
  void requiredStartsNewAndMandatoryRejectsWhenNoTransactionExists() {
    List<TransactionDefinition> calls = new ArrayList<>();
    TransactionOperations<Object> transactions = transactions(calls);
    doReturn(Optional.empty()).when(transactions).findTransactionStatus();
    TestStore proxy =
        (TestStore)
            RatchetTransactionalStoreProxy.createProxy(
                new AnnotatedStore(), TestStore.class, transactions);
    proxy.required();
    assertEquals(Propagation.REQUIRES_NEW, calls.get(0).getPropagationBehavior());
    assertThrows(IllegalTransactionStateException.class, proxy::mandatory);
    assertEquals(1, calls.size());
  }

  @Test
  void unannotatedMethodsStayUnadvisedAndDefaultCallsRemainAdvised() throws Exception {
    List<TransactionDefinition> calls = new ArrayList<>();
    var transactions = transactions(calls);
    TestStore proxy =
        (TestStore)
            RatchetTransactionalStoreProxy.createProxy(
                new MethodOnlyStore(), TestStore.class, transactions);
    proxy.required();
    assertTrue(calls.isEmpty());
    proxy.nested();
    assertEquals(
        List.of(Propagation.REQUIRES_NEW),
        calls.stream().map(TransactionDefinition::getPropagationBehavior).toList());
    assertSame(proxy, proxy.capability(TestStore.class).orElseThrow());
    assertTrue(proxy.capability(Runnable.class).isEmpty());
    assertSame(proxy, proxy);
    assertTrue(proxy.equals(proxy));
    assertFalse(proxy.equals(new MethodOnlyStore()));
    proxy.hashCode();
    proxy.toString();
    verify(transactions, times(1)).execute(any(), any());
  }

  @Test
  void preservesTransientStoreFailureWhenTransactionCleanupReplacesIt() {
    var original = new RatchetTransientStoreException("store", new IOException("connection"));
    var boundary = new IllegalStateException("rollback failed");
    TransactionOperations<Object> transactions = mock(TransactionOperations.class);
    doAnswer(
            invocation -> {
              TransactionCallback<Object, Object> callback = invocation.getArgument(1);
              assertThrows(
                  RatchetTransientStoreException.class,
                  () -> callback.call(mock(TransactionStatus.class)));
              throw boundary;
            })
        .when(transactions)
        .execute(any(), any());
    TestStore proxy =
        (TestStore)
            RatchetTransactionalStoreProxy.createProxy(
                new AnnotatedStore() {
                  @Override
                  public void required() {
                    throw original;
                  }
                },
                TestStore.class,
                transactions);
    var failure = assertThrows(RatchetTransientStoreException.class, proxy::required);
    assertSame(boundary, failure.getCause());
    assertSame(original, failure.getSuppressed()[0]);
  }

  private static TransactionDefinition definition(String name) throws Exception {
    Method method = AnnotatedStore.class.getMethod(name);
    Transactional annotation = method.getAnnotation(Transactional.class);
    if (annotation == null) annotation = AnnotatedStore.class.getAnnotation(Transactional.class);
    return RatchetTransactionalStoreProxy.definition(annotation);
  }

  @SuppressWarnings("unchecked")
  private static TransactionOperations<Object> transactions(List<TransactionDefinition> calls) {
    TransactionOperations<Object> transactions = mock(TransactionOperations.class);
    doReturn(Optional.of(mock(TransactionStatus.class))).when(transactions).findTransactionStatus();
    doAnswer(
            invocation -> {
              calls.add(invocation.getArgument(0));
              TransactionCallback<Object, Object> callback = invocation.getArgument(1);
              return callback.call(mock(TransactionStatus.class));
            })
        .when(transactions)
        .execute(any(), any());
    return transactions;
  }

  interface TestStore {
    void required();

    void requiresNew();

    void mandatory();

    void supports();

    void notSupported();

    void never();

    void checked() throws IOException;

    void excluded();

    void overlap() throws IOException;

    default void nested() {
      requiresNew();
    }

    default <T> Optional<T> capability(Class<T> type) {
      return Optional.empty();
    }
  }

  @Transactional
  static class AnnotatedStore implements TestStore {
    public void required() {}

    @Transactional(TxType.REQUIRES_NEW)
    public void requiresNew() {}

    @Transactional(TxType.MANDATORY)
    public void mandatory() {}

    @Transactional(TxType.SUPPORTS)
    public void supports() {}

    @Transactional(TxType.NOT_SUPPORTED)
    public void notSupported() {}

    @Transactional(TxType.NEVER)
    public void never() {}

    @Transactional(rollbackOn = IOException.class)
    public void checked() throws IOException {
      throw new IOException("checked");
    }

    @Transactional(dontRollbackOn = IllegalArgumentException.class)
    public void excluded() {
      throw new IllegalArgumentException("excluded");
    }

    @Transactional(rollbackOn = IOException.class, dontRollbackOn = Exception.class)
    public void overlap() throws IOException {
      throw new IOException("overlap");
    }
  }

  static class MethodOnlyStore implements TestStore {
    public void required() {}

    @Transactional(TxType.REQUIRES_NEW)
    public void requiresNew() {}

    public void mandatory() {}

    public void supports() {}

    public void notSupported() {}

    public void never() {}

    public void checked() {}

    public void excluded() {}

    public void overlap() {}
  }
}
