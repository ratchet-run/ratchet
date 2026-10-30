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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.EntityManagerFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.junit.jupiter.api.Test;
import run.ratchet.store.schema.RatchetJpaModel;

class MysqlHibernateSchemaValidationTest {

  @Test
  void validatesSchemaWithoutMappingFile() {
    // Mirrors a Jakarta EE Hibernate deployment on MySQL, which uses no mapping file
    // (website/docs/deployment/mysql.md), and guards the scheduler_job.priority type.
    StandardServiceRegistry registry =
        new StandardServiceRegistryBuilder()
            .applySetting("jakarta.persistence.jdbc.url", MysqlTestFixture.jdbcUrl())
            .applySetting("jakarta.persistence.jdbc.user", MysqlTestFixture.username())
            .applySetting("jakarta.persistence.jdbc.password", MysqlTestFixture.password())
            .applySetting("hibernate.transaction.coordinator_class", "jdbc")
            .applySetting("hibernate.hbm2ddl.auto", "validate")
            .build();
    try {
      MetadataSources sources = new MetadataSources(registry);
      RatchetJpaModel.ENTITY_CLASS_NAMES.forEach(sources::addAnnotatedClassName);
      RatchetJpaModel.CONVERTER_CLASS_NAMES.forEach(sources::addAnnotatedClassName);
      try (EntityManagerFactory factory =
          assertDoesNotThrow(() -> sources.buildMetadata().buildSessionFactory())) {
        assertTrue(factory.isOpen());
      }
    } finally {
      StandardServiceRegistryBuilder.destroy(registry);
    }
  }
}
