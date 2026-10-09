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
package run.ratchet.micronaut.jpa;

import org.hibernate.Version;

/** Hibernate-specific XML overrides, shared by the Hibernate 6 and 7 boot paths. */
final class HibernateJpaMappings {
  private HibernateJpaMappings() {}

  static String mappingFile(SqlStoreVendor vendor) {
    return switch (vendor) {
      case POSTGRESQL -> vendor.jpaMappingFile();
      case MYSQL -> "META-INF/ratchet/hibernate/orm-mysql.xml";
      case ORACLE -> "META-INF/ratchet/hibernate/orm-oracle.xml";
      case SQLSERVER -> sqlserverHibernateMappingFile(hibernateMajorVersion());
    };
  }

  // The XMLs are ported unchanged from Spring and use each Hibernate major's UUID JDBC-type
  // annotation syntax.
  static String sqlserverHibernateMappingFile(int major) {
    return switch (major) {
      case 6 -> "META-INF/ratchet/hibernate/orm-sqlserver-6.xml";
      case 7 -> "META-INF/ratchet/hibernate/orm-sqlserver-7.xml";
      default ->
          throw new IllegalStateException(
              "Ratchet SQL Server BINARY(16) UUID mappings require Hibernate 6 or 7, but found "
                  + major);
    };
  }

  private static int hibernateMajorVersion() {
    String version = Version.getVersionString();
    try {
      return Integer.parseInt(version.substring(0, version.indexOf('.')));
    } catch (RuntimeException failure) {
      throw new IllegalStateException("Cannot determine Hibernate version: " + version, failure);
    }
  }
}
