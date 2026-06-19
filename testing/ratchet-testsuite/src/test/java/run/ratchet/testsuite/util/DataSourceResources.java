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
package run.ratchet.testsuite.util;

final class DataSourceResources {

  private DataSourceResources() {}

  static String dataSourceClassName(String dbType) {
    return switch (dbType) {
      case "mysql" -> "com.mysql.cj.jdbc.MysqlDataSource";
      case "postgresql" -> "org.postgresql.ds.PGSimpleDataSource";
      case "oracle" -> "oracle.jdbc.datasource.OracleDataSource";
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  static String driverCoordinates(String dbType) {
    return switch (dbType) {
      case "mysql" -> "com.mysql:mysql-connector-j";
      case "postgresql" -> "org.postgresql:postgresql";
      case "oracle" -> "com.oracle.database.jdbc:ojdbc11";
      default -> throw new IllegalArgumentException("Unsupported database type: " + dbType);
    };
  }

  static String xml(String value) {
    return value
        .replace("&", "&amp;")
        .replace("\"", "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;");
  }
}
