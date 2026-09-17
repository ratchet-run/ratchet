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

import jakarta.persistence.spi.PersistenceProvider;
import org.hibernate.Version;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.springframework.util.ClassUtils;

/** Optional provider adaptations, kept out of the shared stores and JPA infrastructure. */
final class HibernateJpaMappings {
  private HibernateJpaMappings() {}

  static String mappingFile(PersistenceProvider provider, SqlStoreVendor vendor) {
    if (!ClassUtils.isPresent(
            "org.hibernate.jpa.HibernatePersistenceProvider",
            HibernateJpaMappings.class.getClassLoader())
        || !(provider instanceof HibernatePersistenceProvider)) {
      return vendor.jpaMappingFile();
    }
    return switch (vendor) {
      case POSTGRESQL -> vendor.jpaMappingFile();
      case MYSQL -> "META-INF/ratchet/hibernate/orm-mysql.xml";
      case ORACLE -> "META-INF/ratchet/hibernate/orm-oracle.xml";
      case SQLSERVER -> sqlserverHibernateMappingFile(hibernateMajorVersion());
    };
  }

  static String sqlserverHibernateMappingFile(int hibernateMajorVersion) {
    return switch (hibernateMajorVersion) {
      case 6 -> "META-INF/ratchet/hibernate/orm-sqlserver-6.xml";
      case 7 -> "META-INF/ratchet/hibernate/orm-sqlserver-7.xml";
      default ->
          throw new IllegalStateException(
              "Ratchet SQL Server BINARY(16) UUID mappings require Hibernate 6 or 7, but found "
                  + hibernateMajorVersion);
    };
  }

  private static int hibernateMajorVersion() {
    String version = Version.getVersionString();
    int separator = version.indexOf('.');
    if (separator < 1) {
      throw new IllegalStateException(
          "Ratchet could not determine the Hibernate version: " + version);
    }
    try {
      return Integer.parseInt(version.substring(0, separator));
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Ratchet could not determine the Hibernate version: " + version, e);
    }
  }
}
