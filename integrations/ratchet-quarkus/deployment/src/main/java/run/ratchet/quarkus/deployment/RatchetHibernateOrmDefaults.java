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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Supplies build-time Hibernate ORM defaults for Ratchet's SQL persistence unit. */
public final class RatchetHibernateOrmDefaults implements SmallRyeConfigBuilderCustomizer {

  private static final List<String> BASE_PACKAGES =
      List.of("run.ratchet.store.entity", "run.ratchet.store.converter", "run.ratchet.store.id");

  private static final String RATCHET_PACKAGES =
      Stream.concat(
              BASE_PACKAGES.stream(),
              RatchetSqlProcessor.SQL_STORE_ARTIFACTS.stream()
                  .map(RatchetSqlProcessor.SqlStoreArtifact::converterPackage)
                  .filter(Objects::nonNull))
          .collect(Collectors.joining(","));

  private static final Map<String, String> DEFAULTS =
      Map.of(
          "quarkus.hibernate-orm.\"ratchet\".packages", RATCHET_PACKAGES,
          "quarkus.hibernate-orm.\"ratchet\".datasource", "<default>",
          "quarkus.hibernate-orm.\"ratchet\".database.generation", "none",
          "quarkus.hibernate-orm.\"ratchet\".validate-in-dev-mode", "false",
          "quarkus.hibernate-orm.\"ratchet\".jdbc.timezone", "UTC",
          "quarkus.hibernate-orm.\"ratchet\".mapping-files", "no-file");

  @Override
  public void configBuilder(SmallRyeConfigBuilder builder) {
    builder.withDefaultValues(DEFAULTS);
  }
}
