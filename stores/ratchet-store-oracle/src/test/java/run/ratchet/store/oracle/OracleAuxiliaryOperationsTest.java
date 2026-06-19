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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.oracle.converter.UuidRawConverter;

class OracleAuxiliaryOperationsTest {

  @Test
  void tryAcquirePermit_sameJobExistingPermitIsIdempotent() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    OracleAuxiliaryOperations operations =
        new OracleAuxiliaryOperations(entityManagerWithExistingPermit(jobId));

    assertTrue(operations.tryAcquirePermit("res", jobId, "node-1"));
  }

  @Test
  void getPermitRetryDelayUsesDefaultForMissingResource() {
    OracleAuxiliaryOperations operations =
        new OracleAuxiliaryOperations(new OracleStoreContext(entityManagerWithNoResult(), null));

    assertEquals(
        ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS, operations.getPermitRetryDelay("missing"));
  }

  private static EntityManager entityManagerWithNoResult() {
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "setParameter" -> proxy;
                    case "getSingleResult" -> throw new NoResultException();
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return (EntityManager)
        Proxy.newProxyInstance(
            EntityManager.class.getClassLoader(),
            new Class<?>[] {EntityManager.class},
            (proxy, method, args) -> {
              if ("createNativeQuery".equals(method.getName())) {
                return query;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static OracleStoreContext entityManagerWithExistingPermit(UUID expectedJobId) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "createNativeQuery" -> queryReturningExistingPermit(expectedJobId);
                    case "persist" ->
                        throw new AssertionError("existing permit must not insert another row");
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return new OracleStoreContext(em, null);
  }

  private static Query queryReturningExistingPermit(UUID expectedJobId) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> {
                  if (args.length > 1 && args[1] instanceof byte[] actual) {
                    byte[] expected = UuidRawConverter.toBytes(expectedJobId);
                    if (!java.util.Arrays.equals(expected, actual)) {
                      throw new AssertionError("job_id parameter should use Oracle UUID bytes");
                    }
                  }
                  yield proxy;
                }
                // First query: the FOR UPDATE row lock returns max_concurrent = 2.
                case "getResultList" -> Collections.singletonList(2);
                // Second query: existing-permit-for-job count = 1, so acquire is idempotent and
                // must short-circuit before the active-count query or any INSERT.
                case "getSingleResult" -> 1;
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
