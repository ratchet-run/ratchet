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
package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.DuplicateIdempotencyKeyException;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class PostgresqlConstraintDetectorTest {

  private final PostgresqlConstraintDetector detector = new PostgresqlConstraintDetector();

  @Test
  void idempotencyConflictReleasesSameCallBusinessReservationWithoutJobWrites() {
    EntityManager em = mock(EntityManager.class);
    Query cold = mock(Query.class, RETURNS_SELF);
    Query hot = mock(Query.class, RETURNS_SELF);
    Query business = mock(Query.class, RETURNS_SELF);
    Query idempotency = mock(Query.class, RETURNS_SELF);
    Query release = mock(Query.class, RETURNS_SELF);
    when(em.createNativeQuery(anyString())).thenReturn(cold, hot, business, idempotency, release);
    when(business.executeUpdate()).thenReturn(1);
    when(idempotency.executeUpdate()).thenReturn(0);
    PostgresqlStoreContext ctx = new PostgresqlStoreContext(em);
    PostgresqlJobWriteOperations writes =
        new PostgresqlJobWriteOperations(ctx, new PostgresqlBusinessKeyReservations(ctx), null);
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.SINGLE);
    job.setBusinessKey("fresh-business-key");
    job.setIdempotencyKey(UUID.randomUUID().toString());

    DuplicateIdempotencyKeyException conflict =
        assertThrows(DuplicateIdempotencyKeyException.class, () -> writes.saveInsert(job));

    assertNull(conflict.getCause());
    assertEquals(job.getIdempotencyKey(), conflict.idempotencyKey());
    verify(release).executeUpdate();
    verify(cold, never()).executeUpdate();
    verify(hot, never()).executeUpdate();
    verify(em, times(5)).createNativeQuery(anyString());
  }

  @Test
  void reservationConflictWritesNothingForTheJob() {
    EntityManager em = mock(EntityManager.class);
    Query cold = mock(Query.class, RETURNS_SELF);
    Query hot = mock(Query.class, RETURNS_SELF);
    Query reserve = mock(Query.class, RETURNS_SELF);
    when(em.createNativeQuery(anyString())).thenReturn(cold, hot, reserve);
    when(reserve.executeUpdate()).thenReturn(0);
    PostgresqlStoreContext ctx = new PostgresqlStoreContext(em);
    PostgresqlJobWriteOperations writes =
        new PostgresqlJobWriteOperations(ctx, new PostgresqlBusinessKeyReservations(ctx), null);
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.SINGLE);
    job.setBusinessKey("order-1");
    job.setIdempotencyKey(UUID.randomUUID().toString());

    BusinessKeyReservationConflictException thrown =
        assertThrows(BusinessKeyReservationConflictException.class, () -> writes.saveInsert(job));

    assertNull(thrown.getCause());
    verify(cold, never()).executeUpdate();
    verify(hot, never()).executeUpdate();
    verify(em, times(3)).createNativeQuery(anyString());
  }

  @Test
  void doesNotTranslateUnrelatedWriteFailureToReservationConflict() {
    RuntimeException failure = new RuntimeException("unrelated write failure");
    EntityManager em = mock(EntityManager.class);
    when(em.createNativeQuery(anyString())).thenThrow(failure);
    PostgresqlJobWriteOperations writes =
        new PostgresqlJobWriteOperations(new PostgresqlStoreContext(em), null, null);
    JobEntity job = new JobEntity();
    job.setBusinessKey("order-1");

    assertSame(failure, assertThrows(RuntimeException.class, () -> writes.saveInsert(job)));
  }

  @Test
  void extractsConstraintNameThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException(
                "duplicate key value violates unique constraint"
                    + " \"scheduler_business_key_reservation_pkey\"",
                "23505"));

    assertEquals("scheduler_business_key_reservation_pkey", detector.constraintName(wrapped));
  }

  @Test
  void returnsNullWhenConstraintNameIsAbsent() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("duplicate key", "23505"));

    assertNull(detector.constraintName(wrapped));
  }

  @Test
  void detectsDuplicateKeyBySqlStateThroughWrappers() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("duplicate key", "23505"));

    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void detectsDuplicateKeyByMessageThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException("duplicate key value violates unique constraint \"uq_job_key\""));

    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void ignoresNonDuplicateSqlExceptions() {
    Exception wrapped = new RuntimeException("hibernate", new SQLException("foreign key", "23503"));

    assertFalse(detector.isDuplicateKey(wrapped));
  }

  @Test
  void detectsDuplicateBusinessKeyFromReservationConstraint() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException(
                "duplicate key value violates unique constraint"
                    + " \"scheduler_business_key_reservation_pkey\"",
                "23505"));

    assertTrue(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void ignoresDuplicateKeyFromOtherConstraintAsBusinessKey() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException(
                "duplicate key value violates unique constraint \"uq_job_key\"", "23505"));

    assertFalse(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void detectsDeadlockThroughWrappers() {
    Exception wrapped = new RuntimeException("hibernate", new SQLException("deadlock", "40P01"));

    assertTrue(detector.isDeadlock(wrapped));
  }

  @Test
  void detectsSerializationFailureAsRetryableConflict() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("serialization failure", "40001"));

    assertTrue(detector.isDeadlock(wrapped));
  }

  @Test
  void ignoresNonDeadlockSqlExceptions() {
    Exception wrapped = new RuntimeException("hibernate", new SQLException("syntax", "42601"));

    assertFalse(detector.isDeadlock(wrapped));
  }

  @Test
  void detectsConnectionStateThroughWrappers() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("connection lost", "08006"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsAdminShutdownThroughWrappers() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("shutdown", "57P01"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsCrashShutdownThroughWrappers() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("crash shutdown", "57P02"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsCannotConnectNowThroughWrappers() {
    Exception wrapped =
        new RuntimeException("jpa", new SQLException("cannot connect now", "57P03"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsRecoverableSqlExceptions() {
    Exception wrapped = new RuntimeException("jpa", new SQLRecoverableException("recoverable"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsTransientSqlExceptions() {
    Exception wrapped = new RuntimeException("jpa", new SQLTransientException("transient"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void ignoresNonTransientSqlExceptions() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("unique", "23505"));

    assertFalse(detector.isTransientConnectionFailure(wrapped));
  }
}
