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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import org.junit.jupiter.api.Test;

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
