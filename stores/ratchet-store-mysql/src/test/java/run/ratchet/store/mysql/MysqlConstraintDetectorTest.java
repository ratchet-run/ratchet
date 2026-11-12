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

import com.mysql.cj.exceptions.CJCommunicationsException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.SQLException;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.store.BusinessKeyReservationConflictException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

class MysqlConstraintDetectorTest {

  private final MysqlConstraintDetector detector = new MysqlConstraintDetector();

  @Test
  void reservationConflictWritesNothingForTheJob() {
    EntityManager em = mock(EntityManager.class);
    Query cold = mock(Query.class, RETURNS_SELF);
    Query hot = mock(Query.class, RETURNS_SELF);
    Query reserve = mock(Query.class, RETURNS_SELF);
    when(em.createNativeQuery(anyString())).thenReturn(cold, hot, reserve);
    when(reserve.executeUpdate()).thenReturn(0);
    MysqlStoreContext ctx = new MysqlStoreContext(em, null);
    MysqlJobWriteOperations writes =
        new MysqlJobWriteOperations(ctx, new MysqlBusinessKeyReservations(ctx), null);
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
    MysqlJobWriteOperations writes =
        new MysqlJobWriteOperations(new MysqlStoreContext(em, null), null, null);
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
                "Duplicate entry 'abc' for key 'scheduler_business_key_reservation.PRIMARY'"));

    assertEquals("scheduler_business_key_reservation.PRIMARY", detector.constraintName(wrapped));
  }

  @Test
  void returnsNullWhenConstraintNameIsAbsent() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("Duplicate entry 'abc'"));

    assertNull(detector.constraintName(wrapped));
  }

  @Test
  void detectsDuplicateKeyThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("Duplicate entry 'abc' for key 'uq_job_key'"));

    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void ignoresNonDuplicateSqlExceptions() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("Cannot delete or update a parent row", "23000"));

    assertFalse(detector.isDuplicateKey(wrapped));
  }

  @Test
  void detectsDuplicateBusinessKeyFromReservationConstraint() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException(
                "Duplicate entry 'order-1' for key 'scheduler_business_key_reservation.PRIMARY'"));

    assertTrue(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void ignoresDuplicateKeyFromOtherConstraintAsBusinessKey() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("Duplicate entry 'abc' for key 'uq_job_key'"));

    assertFalse(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void detectsDeadlockByErrorCodeThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate", new SQLException("transaction rolled back", "40001", 1213));

    assertTrue(detector.isDeadlock(wrapped));
  }

  @Test
  void detectsLockWaitTimeoutByMessageThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException("Lock wait timeout exceeded; try restarting transaction"));

    assertTrue(detector.isDeadlock(wrapped));
  }

  @Test
  void ignoresNonDeadlockSqlExceptions() {
    Exception wrapped = new RuntimeException("hibernate", new SQLException("syntax", "42000"));

    assertFalse(detector.isDeadlock(wrapped));
  }

  @Test
  void detectsSqlState08PrefixThroughWrappers() {
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08S01")));
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08P01")));
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08999")));
  }

  @Test
  void ignoresSqlStatesThatDoNotStartWith08() {
    assertFalse(detector.isTransientConnectionFailure(wrappedSqlState("00801")));
  }

  @Test
  void detectsMySqlConnectionErrorCodeThroughWrappers() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("server gone away", "HY000", 2006));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void detectsCommunicationsExceptionByClassNameThroughWrappers() {
    Exception wrapped = new RuntimeException("hibernate", new CJCommunicationsException("reset"));

    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void ignoresNonTransientSqlExceptions() {
    Exception wrapped = new RuntimeException("hibernate", new SQLException("syntax", "42000"));

    assertFalse(detector.isTransientConnectionFailure(wrapped));
  }

  private static Exception wrappedSqlState(String sqlState) {
    return new RuntimeException("jpa", new SQLException("connection lost", sqlState));
  }
}
