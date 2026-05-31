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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.lang.reflect.Proxy;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

class MysqlBusinessKeyReservationsTest {

  @Test
  void insertReservationRequiresBusinessKey() {
    MysqlBusinessKeyReservations reservations =
        new MysqlBusinessKeyReservations(new MysqlStoreContext(null, null));

    assertThrows(
        NullPointerException.class,
        () ->
            reservations.insertReservation(
                null,
                UUID.fromString("00000000-0000-0000-0000-000000000001"),
                MysqlBusinessKeyReservations.OWNER_TABLE_QUEUE));
  }

  @Test
  void bindInsertRequiresBusinessKeyBeforeBindingAnyParameters() {
    MysqlBusinessKeyReservations reservations =
        new MysqlBusinessKeyReservations(new MysqlStoreContext(null, null));
    JobEntity job = new JobEntity();
    job.setId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    job.setJobType(JobExecutionType.SINGLE);
    AtomicInteger parameterBindings = new AtomicInteger();
    Query query = queryCountingParameterBindings(parameterBindings);

    assertThrows(
        NullPointerException.class,
        () -> reservations.bindInsert(query, job, Timestamp.from(Instant.EPOCH)));
    assertEquals(0, parameterBindings.get());
  }

  @Test
  void deleteReservationsByOwnersDoesNothingForEmptyList() {
    AtomicInteger nativeQueries = new AtomicInteger();
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("createNativeQuery")) {
                    nativeQueries.incrementAndGet();
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    MysqlBusinessKeyReservations reservations =
        new MysqlBusinessKeyReservations(new MysqlStoreContext(em, null));

    reservations.deleteReservationsByOwners(List.of());

    assertEquals(0, nativeQueries.get());
  }

  @Test
  void deleteReservationsByOwnersBindsUuidBytes() {
    UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
    List<Object> boundValues = new ArrayList<>();
    AtomicInteger executeUpdates = new AtomicInteger();
    Query query =
        (Query)
            Proxy.newProxyInstance(
                Query.class.getClassLoader(),
                new Class<?>[] {Query.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("setParameter")) {
                    boundValues.add(args[1]);
                    return proxy;
                  }
                  if (method.getName().equals("executeUpdate")) {
                    executeUpdates.incrementAndGet();
                    return 2;
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    List<String> sql = new ArrayList<>();
    EntityManager em =
        (EntityManager)
            Proxy.newProxyInstance(
                EntityManager.class.getClassLoader(),
                new Class<?>[] {EntityManager.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("createNativeQuery")) {
                    sql.add((String) args[0]);
                    return query;
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    MysqlBusinessKeyReservations reservations =
        new MysqlBusinessKeyReservations(new MysqlStoreContext(em, null));

    reservations.deleteReservationsByOwners(List.of(first, second));

    assertEquals(
        "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN (?,?)", sql.get(0));
    assertArrayEquals(UuidByteArrayConverter.toBytes(first), (byte[]) boundValues.get(0));
    assertArrayEquals(UuidByteArrayConverter.toBytes(second), (byte[]) boundValues.get(1));
    assertEquals(1, executeUpdates.get());
  }

  private static Query queryCountingParameterBindings(AtomicInteger parameterBindings) {
    return (Query)
        Proxy.newProxyInstance(
            Query.class.getClassLoader(),
            new Class<?>[] {Query.class},
            (proxy, method, args) -> {
              if (method.getName().equals("setParameter")) {
                parameterBindings.incrementAndGet();
                return proxy;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
