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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import run.ratchet.tck.store.AbstractSchemaConformanceContract;
import run.ratchet.tck.store.schema.DialectTypeMapper;

/**
 * SQL Server conformance test for the canonical Ratchet schema. Owns its own Testcontainer (with
 * reuse enabled) so the conformance check is independent of the JPA fixture used by the rest of the
 * TCK suite.
 */
class SqlserverSchemaConformanceContractTest extends AbstractSchemaConformanceContract {

  @SuppressWarnings({"resource", "rawtypes"})
  private static final MSSQLServerContainer CONTAINER = MssqlContainers.shared();

  @Override
  protected Connection openConnection() throws SQLException {
    return DriverManager.getConnection(
        CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
  }

  @Override
  protected DialectTypeMapper mapper() {
    return new SqlserverDialectMapper();
  }
}
