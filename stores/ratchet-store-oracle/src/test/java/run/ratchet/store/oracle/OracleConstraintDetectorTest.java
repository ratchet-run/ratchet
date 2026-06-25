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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import org.junit.jupiter.api.Test;

class OracleConstraintDetectorTest {

  private final OracleConstraintDetector detector = new OracleConstraintDetector();

  private static Exception ora(String message, String sqlState, int errorCode) {
    return new RuntimeException("hibernate", new SQLException(message, sqlState, errorCode));
  }

  @Test
  void extractsConstraintNameStrippedAndLowerCasedThroughWrappers() {
    Exception wrapped =
        ora(
            "ORA-00001: unique constraint (RATCHET.PK_SCHEDULER_BUSINESS_KEY_RESERVATION) violated",
            "23000",
            1);

    assertEquals("pk_scheduler_business_key_reservation", detector.constraintName(wrapped));
  }

  @Test
  void returnsNullWhenConstraintNameIsAbsent() {
    assertNull(
        detector.constraintName(ora("ORA-00942: table or view does not exist", "42000", 942)));
  }

  @Test
  void detectsDuplicateKeyByErrorCode() {
    assertTrue(detector.isDuplicateKey(ora("ORA-00001: unique constraint violated", "23000", 1)));
  }

  @Test
  void detectsDuplicateKeyByMessage() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLException("... ORA-00001: unique constraint ..."));
    assertTrue(detector.isDuplicateKey(wrapped));
  }

  @Test
  void ignoresNonDuplicateSqlExceptions() {
    assertFalse(
        detector.isDuplicateKey(ora("ORA-02291: integrity constraint violated", "23000", 2291)));
  }

  @Test
  void detectsDuplicateBusinessKeyFromReservationConstraint() {
    Exception wrapped =
        ora(
            "ORA-00001: unique constraint (RATCHET.PK_SCHEDULER_BUSINESS_KEY_RESERVATION) violated",
            "23000",
            1);
    assertTrue(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void ignoresDuplicateKeyFromOtherConstraintAsBusinessKey() {
    Exception wrapped =
        ora("ORA-00001: unique constraint (RATCHET.UK_IDEMPOTENCY_KEY) violated", "23000", 1);
    assertFalse(detector.isDuplicateBusinessKey(wrapped));
  }

  @Test
  void detectsDuplicateIdempotencyKeyFromConstraint() {
    Exception wrapped =
        ora("ORA-00001: unique constraint (RATCHET.UK_IDEMPOTENCY_KEY) violated", "23000", 1);
    assertTrue(detector.isDuplicateIdempotencyKey(wrapped));
  }

  @Test
  void detectsDeadlockByErrorCode() {
    assertTrue(detector.isDeadlock(ora("ORA-00060: deadlock detected", "61000", 60)));
  }

  @Test
  void detectsCannotSerializeAsDeadlock() {
    assertTrue(detector.isDeadlock(ora("ORA-08177: can't serialize access", "72000", 8177)));
  }

  @Test
  void ignoresNonDeadlockSqlExceptions() {
    assertFalse(detector.isDeadlock(ora("ORA-00942: table or view does not exist", "42000", 942)));
  }

  @Test
  void detectsSqlState08PrefixThroughWrappers() {
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08S01")));
    assertTrue(detector.isTransientConnectionFailure(wrappedSqlState("08006")));
  }

  @Test
  void ignoresSqlStatesThatDoNotStartWith08() {
    assertFalse(detector.isTransientConnectionFailure(wrappedSqlState("00801")));
  }

  @Test
  void detectsOracleConnectionErrorCodeThroughWrappers() {
    assertTrue(
        detector.isTransientConnectionFailure(
            ora("ORA-03113: end-of-file on communication channel", null, 3113)));
    assertTrue(
        detector.isTransientConnectionFailure(ora("ORA-12541: TNS:no listener", null, 12541)));
  }

  @Test
  void detectsRecoverableExceptionThroughWrappers() {
    Exception wrapped =
        new RuntimeException("hibernate", new SQLRecoverableException("connection reset"));
    assertTrue(detector.isTransientConnectionFailure(wrapped));
  }

  @Test
  void ignoresNonTransientSqlExceptions() {
    assertFalse(detector.isTransientConnectionFailure(ora("syntax", "42000", 900)));
  }

  private static Exception wrappedSqlState(String sqlState) {
    return new RuntimeException("jpa", new SQLException("connection lost", sqlState));
  }
}
