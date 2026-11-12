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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

class OracleAuxiliaryOperationsTest {

  @Test
  void tryAcquirePermit_sameJobExistingPermitIsIdempotent() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    Map<String, Map<Integer, Object>> bound = new HashMap<>();
    OracleAuxiliaryOperations operations =
        new OracleAuxiliaryOperations(entityManagerWithExistingPermit(bound));

    assertEquals(AcquireResult.ACQUIRED, operations.tryAcquirePermit("res", jobId, 7L, "node-1"));

    byte[] jobBytes = UuidRawConverter.toBytes(jobId);
    assertEquals("res", bound.get("lock").get(1));
    assertArrayEquals(jobBytes, (byte[]) bound.get("claim").get(1));
    Map<Integer, Object> update = bound.get("update");
    assertArrayEquals(
        UuidRawConverter.toBytes(ResourcePermitEntity.idFor("res", jobId, 7L)),
        (byte[]) update.get(3));
    assertEquals("node-1", update.get(1));
    assertInstanceOf(Timestamp.class, update.get(2));
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

  private static OracleStoreContext entityManagerWithExistingPermit(
      Map<String, Map<Integer, Object>> bound) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "createNativeQuery" ->
                        queryReturningExistingPermit((String) args[0], bound);
                    case "persist" ->
                        throw new AssertionError("existing permit must not insert another row");
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return new OracleStoreContext(em, null);
  }

  private static Query queryReturningExistingPermit(
      String sql, Map<String, Map<Integer, Object>> bound) {
    String kind =
        sql.contains("scheduler_job_queue")
            ? "claim"
            : sql.startsWith("UPDATE scheduler_resource_permit") ? "update" : "lock";
    Map<Integer, Object> parameters = bound.computeIfAbsent(kind, ignored -> new HashMap<>());
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> {
                  parameters.put((Integer) args[0], args[1]);
                  yield proxy;
                }
                case "getResultList" ->
                    kind.equals("claim")
                        ? Collections.singletonList(new Object[] {"RUNNING", 7L})
                        : Collections.singletonList("res");
                case "executeUpdate" -> {
                  if (!kind.equals("update")) {
                    throw new AssertionError("existing permit must not insert another row");
                  }
                  yield 1;
                }
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
