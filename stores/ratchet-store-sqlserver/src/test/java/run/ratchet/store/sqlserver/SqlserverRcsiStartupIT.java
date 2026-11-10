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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.util.IsolationCheckFailedException;

class SqlserverRcsiStartupIT {

  @Test
  void startupRejectsDatabaseWithRcsiOff() throws Exception {
    // Reuse the shared container without changing its RCSI-enabled database or JDBC URL.
    var container = MssqlContainers.shared();
    String database = "ratchet_rcsi_off_" + UUID.randomUUID().toString().replace("-", "");
    try (Connection admin =
            DriverManager.getConnection(
                container.getJdbcUrl(), container.getUsername(), container.getPassword());
        Statement statement = admin.createStatement()) {
      statement.execute("CREATE DATABASE [" + database + "]");
      try {
        statement.execute("ALTER DATABASE [" + database + "] SET READ_COMMITTED_SNAPSHOT OFF");
        var properties = new HashMap<String, Object>();
        properties.put(
            "jakarta.persistence.jdbc.url",
            container.getJdbcUrl().replaceAll("(?i);databaseName=[^;]*", "")
                + ";databaseName="
                + database);
        properties.put("jakarta.persistence.jdbc.user", container.getUsername());
        properties.put("jakarta.persistence.jdbc.password", container.getPassword());
        properties.put("hibernate.hbm2ddl.auto", "none");
        EntityManagerFactory emf =
            Persistence.createEntityManagerFactory("ratchet-sqlserver-tck", properties);
        try {
          EntityManager em = emf.createEntityManager();
          try {
            assertEquals(database, em.createNativeQuery("SELECT DB_NAME()").getSingleResult());
            assertEquals(
                0,
                ((Number)
                        em.createNativeQuery(
                                """
                                SELECT CAST(is_read_committed_snapshot_on AS int)
                                FROM sys.databases WHERE name = DB_NAME()
                                """)
                            .getSingleResult())
                    .intValue());
            assertEquals(
                "read committed",
                em.createNativeQuery(SqlserverJobStoreImpl.ISOLATION_PROBE_SQL).getSingleResult());

            IsolationCheckFailedException failure =
                assertThrows(
                    IsolationCheckFailedException.class,
                    () ->
                        SqlserverJobStoreFactory.create(
                            em, RatchetOptions.defaults(), mock(MetricsCollector.class)));

            assertTrue(failure.getMessage().contains("READ_COMMITTED_SNAPSHOT is 'off'"));
            assertTrue(
                failure
                    .getMessage()
                    .contains("ALTER DATABASE [<db>] SET READ_COMMITTED_SNAPSHOT ON"));
          } finally {
            em.close();
          }
        } finally {
          emf.close();
        }
      } finally {
        statement.execute(
            "ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
        statement.execute("DROP DATABASE [" + database + "]");
      }
    }
  }
}
