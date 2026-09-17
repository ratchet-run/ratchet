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
package run.ratchet.spring.boot.autoconfigure.jpa;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.TemporalType;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import javax.sql.DataSource;

/** Checks application-owned SQL settings without changing the pool or persistence unit. */
final class RatchetJpaPrerequisites {
  private static final DateTimeFormatter UTC_TIMESTAMP =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS", Locale.ROOT).withZone(ZoneOffset.UTC);
  private static final List<Instant> TIMESTAMP_PROBES =
      List.of(
          Instant.parse("1900-01-15T12:34:56.123Z"),
          Instant.parse("2024-01-15T12:34:56.123Z"),
          Instant.parse("2024-07-15T12:34:56.123Z"));

  private RatchetJpaPrerequisites() {}

  static void validateDataSource(DataSource dataSource, SqlStoreVendor vendor) throws SQLException {
    if (vendor != SqlStoreVendor.MYSQL) return;
    try (Connection connection = dataSource.getConnection()) {
      if (connection.getTransactionIsolation() != Connection.TRANSACTION_READ_COMMITTED) {
        throw new IllegalStateException(
            "Ratchet MySQL requires READ_COMMITTED transaction isolation; configure the selected"
                + " DataSource, for example"
                + " spring.datasource.hikari.transaction-isolation=TRANSACTION_READ_COMMITTED");
      }
    }
  }

  static void validateEntityManagerFactory(EntityManagerFactory factory, SqlStoreVendor vendor) {
    if (vendor != SqlStoreVendor.ORACLE && vendor != SqlStoreVendor.SQLSERVER) return;
    String sql =
        switch (vendor) {
          case ORACLE ->
              "SELECT TO_CHAR(CAST(?1 AS TIMESTAMP(3)), 'YYYY-MM-DD HH24:MI:SS.FF3') FROM dual";
          case SQLSERVER -> "SELECT CONVERT(varchar(23), CAST(?1 AS datetime2(3)), 121)";
          default -> throw new IllegalArgumentException("No timestamp probe for " + vendor);
        };
    // Compare a database-formatted value, not a Timestamp read back through the same driver:
    // symmetric write/read timezone shifts would otherwise hide incorrect stored values.
    try (var entityManager = factory.createEntityManager()) {
      for (Instant instant : TIMESTAMP_PROBES) {
        String expected = UTC_TIMESTAMP.format(instant);
        Object actual =
            entityManager
                .createNativeQuery(sql)
                .setParameter(1, Timestamp.from(instant), TemporalType.TIMESTAMP)
                .getSingleResult();
        if (!expected.equals(actual)) {
          throw new IllegalStateException(
              "Ratchet "
                  + vendor
                  + " requires UTC JDBC timestamp binding; the selected "
                  + "EntityManagerFactory bound "
                  + instant
                  + " as "
                  + actual
                  + ". Configure UTC JDBC timestamp binding on the selected persistence unit.");
        }
      }
    }
  }
}
