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
package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;

class MysqlAuxiliaryOperationsTest {

  @Test
  void tryAcquirePermit_sameJobExistingPermitIsIdempotent() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(entityManagerWithExistingPermit(jobId, 0L));

    assertEquals(AcquireResult.ACQUIRED, operations.tryAcquirePermit("res", jobId, 0L, "node-1"));
  }

  @Test
  void tryAcquirePermit_olderClaimCountsAgainstCapacity() {
    UUID jobId = UUID.fromString("01900000-0000-7000-8000-000000000001");
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(entityManagerWithExistingPermit(jobId, 1L));

    assertEquals(
        AcquireResult.AT_CAPACITY, operations.tryAcquirePermit("res", jobId, 1L, "node-1"));
  }

  @Test
  void getPermitRetryDelayUsesDefaultForMissingResource() {
    MysqlAuxiliaryOperations operations =
        new MysqlAuxiliaryOperations(new MysqlStoreContext(entityManagerWithNoResult(), null));

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

  private static MysqlStoreContext entityManagerWithExistingPermit(
      UUID expectedJobId, long claimSeq) {
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  return switch (method.getName()) {
                    case "createNativeQuery" ->
                        queryReturningExistingPermit(expectedJobId, claimSeq, (String) args[0]);
                    case "persist" ->
                        throw new AssertionError("existing permit must not insert another row");
                    default -> throw new UnsupportedOperationException(method.getName());
                  };
                });
    return new MysqlStoreContext(em, null);
  }

  private static Query queryReturningExistingPermit(UUID expectedJobId, long claimSeq, String sql) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              return switch (method.getName()) {
                case "setParameter" -> proxy;
                case "getResultList" ->
                    sql.contains("scheduler_job_queue")
                        ? Collections.singletonList(new Object[] {"RUNNING", claimSeq})
                        : sql.contains("max_concurrent")
                            ? List.of(1)
                            : Collections.singletonList(
                                UuidByteArrayConverter.toBytes(
                                    ResourcePermitEntity.idFor("res", expectedJobId, 0L)));
                default -> throw new UnsupportedOperationException(method.getName());
              };
            });
  }
}
