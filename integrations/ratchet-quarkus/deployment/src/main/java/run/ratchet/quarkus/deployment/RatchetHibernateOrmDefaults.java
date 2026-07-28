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
package run.ratchet.quarkus.deployment;

import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.SmallRyeConfigBuilderCustomizer;
import java.util.Map;

/** Supplies build-time Hibernate ORM defaults for Ratchet's SQL persistence unit. */
public final class RatchetHibernateOrmDefaults implements SmallRyeConfigBuilderCustomizer {

  private static final Map<String, String> DEFAULTS =
      Map.of(
          "quarkus.hibernate-orm.\"ratchet\".packages",
              "run.ratchet.store.entity,run.ratchet.store.converter,run.ratchet.store.id,"
                  + "run.ratchet.store.mysql.converter,run.ratchet.store.oracle.converter,"
                  + "run.ratchet.store.sqlserver.converter",
          "quarkus.hibernate-orm.\"ratchet\".datasource", "<default>",
          "quarkus.hibernate-orm.\"ratchet\".database.generation", "none",
          "quarkus.hibernate-orm.\"ratchet\".validate-in-dev-mode", "false",
          "quarkus.hibernate-orm.\"ratchet\".jdbc.timezone", "UTC",
          "quarkus.hibernate-orm.\"ratchet\".mapping-files", "no-file",
          "quarkus.hibernate-orm.mapping-files", "no-file");

  @Override
  public void configBuilder(SmallRyeConfigBuilder builder) {
    builder.withDefaultValues(DEFAULTS);
  }
}
