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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.GeneratedFiles.Kind;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeReference;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.test.context.FilteredClassLoader;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaAotSettings;

class RatchetJpaAotProcessorTest {
  @Test
  void requiresOneStoreWithoutOpeningADatabase() {
    var factory = factory();
    assertThatThrownBy(() -> new RatchetJpaAotProcessor().processAheadOfTime(factory))
        .hasMessageContaining("exactly one installed SQL store");
  }

  @Test
  void namesTheHibernateLimitForGeneratedJvmAotAndNativeImages() {
    var factory = factory();
    factory.setBeanClassLoader(new FilteredClassLoader("org.hibernate.jpa"));
    assertThatThrownBy(() -> new RatchetJpaAotProcessor().processAheadOfTime(factory))
        .hasMessageContaining("generated JVM AOT and native images", "requires Hibernate");
  }

  @Test
  void generatesOnlyInstalledVendorWithJpaAndProxyMetadata() throws Exception {
    var factory = factory();
    factory.setBeanClassLoader(
        new FilteredClassLoader(
            "run.ratchet.store.mysql", "run.ratchet.store.oracle", "run.ratchet.store.sqlserver"));
    var hints = new RuntimeHints();
    var files = new InMemoryGeneratedFiles();
    var context = mock(GenerationContext.class);
    when(context.getRuntimeHints()).thenReturn(hints);
    when(context.getGeneratedFiles()).thenReturn(files);
    new RatchetJpaAotProcessor().processAheadOfTime(factory).applyTo(context, null);
    new RatchetJpaAotProcessor().processAheadOfTime(factory).applyTo(context, null);
    assertThat(files.getGeneratedFileContent(Kind.RESOURCE, RatchetJpaAotSettings.RESOURCE))
        .contains(
            "vendor=POSTGRESQL", "mapping=META-INF/orm-postgresql.xml", "default-orm=ratchet");
    assertThat(hints.reflection().getTypeHint(SqlStoreVendor.PostgresqlVendor.class)).isNotNull();
    assertThat(hints.reflection().getTypeHint(SqlStoreVendor.MysqlVendor.class)).isNull();
    assertThat(
            RuntimeHintsPredicates.proxies()
                .forInterfaces(jakarta.persistence.spi.PersistenceProvider.class))
        .accepts(hints);
    assertThat(hints.reflection().getTypeHint(run.ratchet.store.entity.JobEntity.class))
        .isNotNull();
    // XML mappings synthesize annotations and use JAXB even when entities have no such annotation.
    assertThat(
            hints
                .reflection()
                .getTypeHint(TypeReference.of("org.hibernate.annotations.JdbcTypeCode"))
                .getMemberCategories())
        .contains(org.springframework.aot.hint.MemberCategory.INVOKE_DECLARED_METHODS);
    assertThat(RuntimeHintsPredicates.proxies().forInterfaces(jakarta.persistence.Column.class))
        .accepts(hints);
    assertThat(RuntimeHintsPredicates.resource().forResource("org/hibernate/jpa/orm_3_1.xsd"))
        .accepts(hints);
    assertThat(
            hints
                .reflection()
                .getTypeHint(
                    TypeReference.of(
                        "jakarta.xml.bind.annotation.adapters.CollapsedStringAdapter")))
        .isNotNull();
    assertThat(factory.containsSingleton("ratchetJpaJobStore")).isFalse();
  }

  @Test
  void usesDefaultClassLoaderWhenBeanFactoryDoesNotProvideOne() throws Exception {
    var factory =
        new DefaultListableBeanFactory() {
          @Override
          public ClassLoader getBeanClassLoader() {
            return null;
          }
        };
    factory.registerBeanDefinition("ratchetJpaJobStore", new RootBeanDefinition(Uncreatable.class));
    var thread = Thread.currentThread();
    var previous = thread.getContextClassLoader();
    try (var loader =
        new FilteredClassLoader(
            "run.ratchet.store.mysql", "run.ratchet.store.oracle", "run.ratchet.store.sqlserver")) {
      thread.setContextClassLoader(loader);
      var hints = new RuntimeHints();
      var files = new InMemoryGeneratedFiles();
      var context = mock(GenerationContext.class);
      when(context.getRuntimeHints()).thenReturn(hints);
      when(context.getGeneratedFiles()).thenReturn(files);
      new RatchetJpaAotProcessor().processAheadOfTime(factory).applyTo(context, null);
      assertThat(files.getGeneratedFileContent(Kind.RESOURCE, RatchetJpaAotSettings.RESOURCE))
          .contains("vendor=POSTGRESQL", "default-orm=ratchet");
      assertThat(hints.reflection().getTypeHint(run.ratchet.store.entity.JobEntity.class))
          .isNotNull();
      assertThat(factory.containsSingleton("ratchetJpaJobStore")).isFalse();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.EnumSource(
      value = SqlStoreVendor.class,
      names = {"ORACLE", "SQLSERVER"})
  void contributesVendorSpecificLobOrCharsetMetadata(SqlStoreVendor vendor) throws Exception {
    var factory = factory();
    var excluded =
        java.util.Arrays.stream(SqlStoreVendor.values())
            .filter(other -> other != vendor)
            .map(other -> "run.ratchet.store." + other.name().toLowerCase(java.util.Locale.ROOT))
            .toArray(String[]::new);
    factory.setBeanClassLoader(new FilteredClassLoader(excluded));
    var context = mock(GenerationContext.class);
    var hints = new RuntimeHints();
    var files = new InMemoryGeneratedFiles();
    when(context.getRuntimeHints()).thenReturn(hints);
    when(context.getGeneratedFiles()).thenReturn(files);
    new RatchetJpaAotProcessor().processAheadOfTime(factory).applyTo(context, null);
    if (vendor == SqlStoreVendor.SQLSERVER) {
      assertThat(
              files.getGeneratedFileContent(
                  Kind.RESOURCE,
                  "META-INF/native-image/run.ratchet/spring-jpa/native-image.properties"))
          .contains("-H:+AddAllCharsets");
    } else {
      assertThat(hints.proxies().jdkProxyHints().map(hint -> hint.getProxiedInterfaces()))
          .anySatisfy(
              interfaces ->
                  assertThat(interfaces)
                      .contains(
                          TypeReference.of(java.sql.Clob.class),
                          TypeReference.of(java.io.Serializable.class)));
    }
    assertThat(factory.containsSingleton("ratchetJpaJobStore")).isFalse();
  }

  @Test
  void runtimeDatabaseMustMatchTheVendorSelectedAtBuildTime() throws Exception {
    var dataSource = mock(javax.sql.DataSource.class);
    var connection = mock(java.sql.Connection.class);
    var metadata = mock(java.sql.DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    try (var settings = org.mockito.Mockito.mockStatic(RatchetJpaAotSettings.class)) {
      settings.when(() -> RatchetJpaAotSettings.get("vendor")).thenReturn("POSTGRESQL");
      when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");
      assertThat(SqlStoreVendor.detect(dataSource)).isEqualTo(SqlStoreVendor.POSTGRESQL);
      when(metadata.getDatabaseProductName()).thenReturn("MySQL");
      assertThatThrownBy(() -> SqlStoreVendor.detect(dataSource))
          .hasMessageContaining("built for POSTGRESQL", "configured database is MYSQL");
    }
  }

  @Test
  void backsOffWhenJpaStoreIsNotConfigured() {
    assertThat(new RatchetJpaAotProcessor().processAheadOfTime(new DefaultListableBeanFactory()))
        .isNull();
  }

  @Test
  void copiesApplicationOwnedMappingWithoutPersistingJarUrls(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
    var mapping = directory.resolve("orm.xml");
    String xml =
        "<entity-mappings xmlns=\"https://jakarta.ee/xml/ns/persistence/orm\" version=\"3.1\"/>";
    java.nio.file.Files.writeString(mapping, xml);
    var factory = factory();
    factory.setBeanClassLoader(
        new FilteredClassLoader(
            "run.ratchet.store.mysql", "run.ratchet.store.oracle", "run.ratchet.store.sqlserver") {
          @Override
          public java.net.URL getResource(String name) {
            try {
              return name.equals("META-INF/orm.xml")
                  ? mapping.toUri().toURL()
                  : super.getResource(name);
            } catch (java.net.MalformedURLException failure) {
              throw new AssertionError(failure);
            }
          }

          @Override
          public java.io.InputStream getResourceAsStream(String name) {
            if (!name.equals("META-INF/orm.xml")) return super.getResourceAsStream(name);
            return new java.io.ByteArrayInputStream(
                xml.getBytes(java.nio.charset.StandardCharsets.UTF_8));
          }
        });
    var context = mock(GenerationContext.class);
    var files = new InMemoryGeneratedFiles();
    when(context.getRuntimeHints()).thenReturn(new RuntimeHints());
    when(context.getGeneratedFiles()).thenReturn(files);
    new RatchetJpaAotProcessor().processAheadOfTime(factory).applyTo(context, null);
    assertThat(files.getGeneratedFileContent(Kind.RESOURCE, RatchetJpaAotSettings.RESOURCE))
        .contains(
            "default-orm=application",
            "application-orm=META-INF/ratchet/spring-application-orm.xml")
        .doesNotContain(directory.toString(), "jar:");
    assertThat(
            files.getGeneratedFileContent(
                Kind.RESOURCE, "META-INF/ratchet/spring-application-orm.xml"))
        .isEqualTo(xml);
  }

  private static DefaultListableBeanFactory factory() {
    var factory = new DefaultListableBeanFactory();
    factory.registerBeanDefinition("ratchetJpaJobStore", new RootBeanDefinition(Uncreatable.class));
    return factory;
  }

  public static class Uncreatable {
    public Uncreatable() {
      throw new AssertionError("AOT created a store");
    }
  }
}
