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
import java.io.IOException;
import org.hibernate.Version;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeReference;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.util.ClassUtils;

/** Optional provider adaptations, kept out of the shared stores and JPA infrastructure. */
final class HibernateJpaMappings {
  private HibernateJpaMappings() {}

  static void registerAotHints(RuntimeHints hints, ClassLoader loader, SqlStoreVendor vendor) {
    if (vendor == SqlStoreVendor.ORACLE) registerLobHints(hints, loader);
    for (String name :
        new String[] {
          "jakarta.xml.bind.annotation.adapters.CollapsedStringAdapter",
          "jakarta.xml.bind.annotation.adapters.NormalizedStringAdapter",
          "jakarta.xml.bind.annotation.adapters.HexBinaryAdapter"
        }) {
      hints
          .reflection()
          .registerType(
              TypeReference.of(name),
              MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
              MemberCategory.INVOKE_DECLARED_METHODS);
    }
    hints.resources().registerPattern("org/hibernate/xsd/**");
    hints.resources().registerPattern("org/hibernate/jpa/*.xsd");
    hints.resources().registerPattern("org/hibernate/*.xsd");
    hints.resources().registerPattern("org/hibernate/*.dtd");
    var resolver = new PathMatchingResourcePatternResolver(loader);
    var metadata = new CachingMetadataReaderFactory(resolver);
    try {
      // JBoss Logging resolves generated implementations by name. Hibernate's XML/UUID paths
      // use additional loggers beyond those covered by the dependency's default native metadata.
      for (var resource : resolver.getResources("classpath*:org/hibernate/**/*_$logger.class")) {
        String name = metadata.getMetadataReader(resource).getClassMetadata().getClassName();
        hints
            .reflection()
            .registerType(TypeReference.of(name), MemberCategory.INVOKE_DECLARED_CONSTRUCTORS);
      }
      hints.reflection().registerType(java.util.UUID[].class);
      for (String root :
          new String[] {
            "org/hibernate/boot/jaxb/mapping",
            "org/hibernate/boot/jaxb/hbm",
            "org/hibernate/boot/jaxb/cfg"
          }) {
        for (var resource : resolver.getResources("classpath*:" + root + "/**/*.class")) {
          String name = metadata.getMetadataReader(resource).getClassMetadata().getClassName();
          hints
              .reflection()
              .registerType(
                  TypeReference.of(name),
                  MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
                  MemberCategory.INVOKE_DECLARED_METHODS,
                  MemberCategory.DECLARED_FIELDS);
        }
      }
      // XML overrides are synthesized as annotation proxies by Hibernate, including annotations
      // that are absent from the Java entity itself. Spring's managed-type hints cannot see them.
      for (String root : new String[] {"jakarta/persistence", "org/hibernate/annotations"}) {
        for (var resource : resolver.getResources("classpath*:" + root + "/**/*.class")) {
          var type = metadata.getMetadataReader(resource).getClassMetadata();
          if (type.isAnnotation()) {
            var reference = TypeReference.of(type.getClassName());
            hints.reflection().registerType(reference, MemberCategory.INVOKE_DECLARED_METHODS);
            hints.proxies().registerJdkProxy(reference);
          }
        }
      }
    } catch (IOException failure) {
      throw new IllegalStateException("Cannot inspect Hibernate XML binding metadata", failure);
    }
  }

  private static void registerLobHints(RuntimeHints hints, ClassLoader loader) {
    String wrappedPackage =
        ClassUtils.isPresent("org.hibernate.engine.jdbc.proxy.WrappedClob", loader)
            ? "org.hibernate.engine.jdbc.proxy."
            : "org.hibernate.engine.jdbc.";
    for (String type : new String[] {"Blob", "Clob", "NClob"}) {
      var jdbc = TypeReference.of("java.sql." + type);
      var wrapped = TypeReference.of(wrappedPackage + "Wrapped" + type);
      hints.reflection().registerType(jdbc, MemberCategory.INVOKE_PUBLIC_METHODS);
      if (type.equals("NClob")) {
        hints.proxies().registerJdkProxy(jdbc, wrapped);
      } else {
        hints
            .proxies()
            .registerJdkProxy(jdbc, wrapped, TypeReference.of(java.io.Serializable.class));
      }
    }
  }

  static String aotMappingFile(SqlStoreVendor vendor) {
    return mappingFile(new HibernatePersistenceProvider(), vendor);
  }

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
