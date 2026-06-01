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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import org.junit.jupiter.api.Test;

class PostgresqlConstraintDetectorTest {

  private final PostgresqlConstraintDetector detector = new PostgresqlConstraintDetector();

  @Test
  void extractsConstraintNameThroughWrappers() {
    Exception wrapped =
        new RuntimeException(
            "hibernate",
            new SQLException(
                "duplicate key value violates unique constraint \"scheduler_business_key_reservation_pkey\"",
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
                "duplicate key value violates unique constraint \"scheduler_business_key_reservation_pkey\"",
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
