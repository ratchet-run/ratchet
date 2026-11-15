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
package run.ratchet.store.sqlserver;

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

class SqlserverConstraintDetectorTest {

  private final SqlserverConstraintDetector detector = new SqlserverConstraintDetector();

  // SQL Server unique/PK violation: SQLState 23000, vendor error 2627 (constraint) / 2601 (index).
  private static SQLException uniqueViolation(String constraint) {
    String name = constraint == null ? "uk_unknown" : constraint;
    return new SQLException(
        "Violation of UNIQUE KEY constraint '"
            + name
            + "'. Cannot insert duplicate key in object 'dbo.scheduler_job'.",
        "23000",
        2627);
  }

  @Test
  void idempotencyConflictReleasesSameCallBusinessReservationWithoutJobWrites() {
    EntityManager em = mock(EntityManager.class);
    Query cold = mock(Query.class, RETURNS_SELF);
    Query hot = mock(Query.class, RETURNS_SELF);
    Query business = mock(Query.class, RETURNS_SELF);
    Query idempotency = mock(Query.class, RETURNS_SELF);
    Query release = mock(Query.class, RETURNS_SELF);
    when(em.createNativeQuery(anyString())).thenReturn(cold, hot, business, idempotency, release);
    when(business.getSingleResult()).thenReturn(1);
    when(idempotency.getSingleResult()).thenReturn(0);
    SqlserverStoreContext ctx = new SqlserverStoreContext(em);
    SqlserverJobWriteOperations writes =
        new SqlserverJobWriteOperations(ctx, new SqlserverBusinessKeyReservations(ctx), null);
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
    when(reserve.getSingleResult()).thenReturn(0);
    SqlserverStoreContext ctx = new SqlserverStoreContext(em);
    SqlserverJobWriteOperations writes =
        new SqlserverJobWriteOperations(ctx, new SqlserverBusinessKeyReservations(ctx), null);
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
    SqlserverJobWriteOperations writes =
        new SqlserverJobWriteOperations(new SqlserverStoreContext(em), null, null);
    JobEntity job = new JobEntity();
    job.setBusinessKey("order-1");

    assertSame(failure, assertThrows(RuntimeException.class, () -> writes.saveInsert(job)));
  }

  @Test
  void extractsConstraintNameThroughWrappers() {
    Exception wrapped =
        new RuntimeException("hibernate", uniqueViolation("pk_scheduler_business_key_reservation"));

    assertEquals("pk_scheduler_business_key_reservation", detector.constraintName(wrapped));
  }

  @Test
  void returnsNullWhenConstraintNameIsAbsent() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("Cannot insert duplicate key", "23000"));

    assertNull(detector.constraintName(wrapped));
  }

  @Test
  void detectsDuplicateKeyByErrorCodeThroughWrappers() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("duplicate", "23000", 2627));

    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void detectsDuplicateKeyByUniqueIndexErrorCode() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("duplicate", "23000", 2601));

    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void detectsDuplicateKeyByMessageThroughWrappers() {
    Exception wrapped = new RuntimeException("hibernate", uniqueViolation("uq_job_key"));

    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void ignoresNonDuplicateSqlExceptions() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("foreign key", "23000", 547));

    assertFalse(detector.isDuplicateKey(wrapped));
  }

  @Test
  void detectsDuplicateBusinessKeyFromReservationConstraint() {
    Exception wrapped =
        new RuntimeException("hibernate", uniqueViolation("pk_scheduler_business_key_reservation"));

    assertTrue(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void ignoresDuplicateKeyFromOtherConstraintAsBusinessKey() {
    Exception wrapped = new RuntimeException("hibernate", uniqueViolation("uq_job_key"));

    assertFalse(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void detectsDeadlockThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("Transaction was deadlocked", "40001", 1205));

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
    Exception wrapped = new RuntimeException("hibernate", new SQLException("syntax", "42000", 102));

    assertFalse(detector.isDeadlock(wrapped));
  }

  @Test
  void detectsConnectionStateThroughWrappers() {
    Exception wrapped = new RuntimeException("jpa", new SQLException("connection lost", "08006"));

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
    Exception wrapped = new RuntimeException("jpa", new SQLException("unique", "23000", 2627));

    assertFalse(detector.isTransientConnectionFailure(wrapped));
  }
}
