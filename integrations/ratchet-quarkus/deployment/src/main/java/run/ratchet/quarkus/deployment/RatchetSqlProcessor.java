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

import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.ExcludedTypeBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.IndexDependencyBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourcePatternsBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.pkg.builditem.CurateOutcomeBuildItem;
import io.quarkus.hibernate.orm.deployment.JpaModelPersistenceUnitContributionBuildItem;
import io.quarkus.hibernate.orm.deployment.spi.AdditionalJpaModelBuildItem;
import io.quarkus.maven.dependency.ResolvedDependency;
import java.util.List;
import run.ratchet.quarkus.runtime.QuarkusRatchetEntityManagerProvider;

/** Hibernate-backed SQL flavor wiring for the existing {@code ratchet-quarkus} artifact. */
class RatchetSqlProcessor {

  private static final String RATCHET_GROUP_ID = "run.ratchet";
  private static final String SCHEMA_MIGRATION_LIFECYCLE_HOOK =
      "run.ratchet.store.migration.SchemaMigrationLifecycleHook";

  static final List<SqlStoreArtifact> SQL_STORE_ARTIFACTS =
      List.of(
          new SqlStoreArtifact(
              "ratchet-store-postgresql",
              "run.ratchet.store.postgresql.PostgresqlSchemaMigrationDialect",
              "run.ratchet.store.postgresql.PostgresqlEntityManagerProvider",
              null),
          new SqlStoreArtifact(
              "ratchet-store-mysql",
              "run.ratchet.store.mysql.MysqlSchemaMigrationDialect",
              "run.ratchet.store.mysql.MysqlEntityManagerProvider",
              "run.ratchet.store.mysql.converter"),
          new SqlStoreArtifact(
              "ratchet-store-oracle",
              "run.ratchet.store.oracle.OracleSchemaMigrationDialect",
              "run.ratchet.store.oracle.OracleEntityManagerProvider",
              "run.ratchet.store.oracle.converter"),
          new SqlStoreArtifact(
              "ratchet-store-sqlserver",
              "run.ratchet.store.sqlserver.SqlserverSchemaMigrationDialect",
              "run.ratchet.store.sqlserver.SqlserverEntityManagerProvider",
              "run.ratchet.store.sqlserver.converter"));

  // Package-private for the orm.xml drift test.
  static final List<String> RATCHET_ENTITY_CLASSES =
      List.of(
          "run.ratchet.store.entity.ArchivedJobEntity",
          "run.ratchet.store.entity.BatchEntity",
          "run.ratchet.store.entity.BatchMetricsEntity",
          "run.ratchet.store.entity.JobEntity",
          "run.ratchet.store.entity.JobExecutionEntity",
          "run.ratchet.store.entity.JobLogEntity",
          "run.ratchet.store.entity.NodeEntity",
          "run.ratchet.store.entity.ResourceLimitEntity",
          "run.ratchet.store.entity.ResourcePermitEntity",
          "run.ratchet.store.entity.WorkflowConditionEntity");

  /** Registers SQL-flavor beans only for the Hibernate-backed extension. */
  @BuildStep
  AdditionalBeanBuildItem beans() {
    return AdditionalBeanBuildItem.builder()
        .addBeanClass(QuarkusRatchetEntityManagerProvider.class)
        .addBeanClass(SCHEMA_MIGRATION_LIFECYCLE_HOOK)
        .setUnremovable()
        .build();
  }

  /**
   * Make ArC discover the active SQL store's dialect bean. Applications choose the concrete {@code
   * ratchet-store-*} dependency, so only index/register artifacts already present.
   */
  @BuildStep
  void sqlStoreBeans(
      CurateOutcomeBuildItem curateOutcome,
      BuildProducer<IndexDependencyBuildItem> indexes,
      BuildProducer<AdditionalBeanBuildItem> beans,
      BuildProducer<ExcludedTypeBuildItem> excludedTypes) {
    for (SqlStoreArtifact store : SQL_STORE_ARTIFACTS) {
      if (hasDependency(curateOutcome, store.artifactId())) {
        indexes.produce(new IndexDependencyBuildItem(RATCHET_GROUP_ID, store.artifactId()));
        excludedTypes.produce(new ExcludedTypeBuildItem(store.entityManagerProviderClass()));
        beans.produce(
            AdditionalBeanBuildItem.builder()
                .addBeanClass(store.dialectClass())
                .setUnremovable()
                .build());
      }
    }
  }

  /**
   * Contributes Ratchet's model to the named Ratchet persistence unit only. Build time config
   * defaults in {@link RatchetHibernateOrmDefaults} disable XML mappings for that unit, while the
   * core extension descriptor removes Ratchet's default-named mapping before Quarkus constructs the
   * augmentation classloader. The application's own mapping resources remain available to its
   * default persistence unit.
   */
  @BuildStep
  void ratchetJpaModel(
      BuildProducer<AdditionalJpaModelBuildItem> additionalJpaModels,
      BuildProducer<JpaModelPersistenceUnitContributionBuildItem> persistenceUnitContributions) {
    RATCHET_ENTITY_CLASSES.forEach(
        className -> additionalJpaModels.produce(new AdditionalJpaModelBuildItem(className)));

    persistenceUnitContributions.produce(
        new JpaModelPersistenceUnitContributionBuildItem(
            QuarkusRatchetEntityManagerProvider.PERSISTENCE_UNIT_NAME,
            null,
            RATCHET_ENTITY_CLASSES,
            List.of("no-file")));
  }

  /** Include bundled schema migrations for {@code ratchet.schema.auto-migrate} in native images. */
  @BuildStep
  NativeImageResourcePatternsBuildItem migrationResources() {
    return NativeImageResourcePatternsBuildItem.builder().includeGlob("ddl/migrations/**").build();
  }

  /** UuidV7EntityListener is instantiated by Hibernate via reflection. */
  @BuildStep
  void hibernateNativeMetadata(BuildProducer<ReflectiveClassBuildItem> reflective) {
    reflective.produce(
        ReflectiveClassBuildItem.builder("run.ratchet.store.id.UuidV7EntityListener")
            .constructors(true)
            .methods(true)
            .fields(true)
            .build());
  }

  private static boolean hasDependency(CurateOutcomeBuildItem curateOutcome, String artifactId) {
    for (ResolvedDependency dependency :
        curateOutcome.getApplicationModel().getRuntimeDependencies()) {
      if (RATCHET_GROUP_ID.equals(dependency.getGroupId())
          && artifactId.equals(dependency.getArtifactId())) {
        return true;
      }
    }
    return false;
  }

  record SqlStoreArtifact(
      String artifactId,
      String dialectClass,
      String entityManagerProviderClass,
      String converterPackage) {}
}
