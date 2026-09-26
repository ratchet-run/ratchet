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
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;
import org.springframework.aot.hint.BindingReflectionHintsRegistrar;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeReference;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotContribution;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;
import org.springframework.util.ClassUtils;
import run.ratchet.spring.boot.autoconfigure.internal.AotResources;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaAotSettings;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;
import run.ratchet.store.schema.RatchetJpaModel;

/** Compiles SQL metadata without obtaining a DataSource or starting the persistence provider. */
public final class RatchetJpaAotProcessor implements BeanFactoryInitializationAotProcessor {
  @Override
  public BeanFactoryInitializationAotContribution processAheadOfTime(
      ConfigurableListableBeanFactory factory) {
    if (!factory.containsBeanDefinition("ratchetJpaJobStore")) return null;
    ClassLoader loader =
        Objects.requireNonNullElseGet(
            factory.getBeanClassLoader(), ClassUtils::getDefaultClassLoader);
    if (!ClassUtils.isPresent("org.hibernate.jpa.HibernatePersistenceProvider", loader)) {
      throw new IllegalStateException(
          "Ratchet SQL AOT (generated JVM AOT and native images) requires Hibernate");
    }
    var installed =
        Arrays.stream(SqlStoreVendor.values())
            .filter(vendor -> ClassUtils.isPresent(vendor.storeClassName(), loader))
            .toList();
    if (installed.size() != 1) {
      throw new IllegalStateException(
          "Ratchet SQL AOT (generated JVM AOT and native images) requires exactly one installed SQL store; found "
              + installed);
    }
    SqlStoreVendor vendor = installed.get(0);
    String mapping = HibernateJpaMappings.aotMappingFile(vendor);
    String ownership = defaultMappingOwnership(loader);
    String applicationOrm = null;
    if ("application".equals(ownership)) {
      try (var stream = loader.getResourceAsStream("META-INF/orm.xml")) {
        applicationOrm =
            new String(Objects.requireNonNull(stream).readAllBytes(), StandardCharsets.UTF_8);
      } catch (IOException failure) {
        throw new IllegalStateException("Cannot copy application-owned orm.xml", failure);
      }
    }
    String applicationMapping = applicationOrm;
    return (generation, initialization) -> {
      var hints = generation.getRuntimeHints();
      HibernateJpaMappings.registerAotHints(hints, loader, vendor);
      if (vendor == SqlStoreVendor.SQLSERVER) {
        // The connection collation selects its code page at runtime.
        AotResources.add(
            generation.getGeneratedFiles(),
            "META-INF/native-image/run.ratchet/spring-jpa/native-image.properties",
            "Args = -H:+AddAllCharsets\n");
      }
      var binding = new BindingReflectionHintsRegistrar();
      Stream.concat(
              RatchetJpaModel.ENTITY_CLASS_NAMES.stream(),
              RatchetJpaModel.CONVERTER_CLASS_NAMES.stream())
          .forEach(
              name -> {
                Class<?> type = ClassUtils.resolveClassName(name, loader);
                binding.registerReflectionHints(hints.reflection(), type);
                hints
                    .reflection()
                    .registerType(
                        type,
                        MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
                        MemberCategory.INVOKE_DECLARED_METHODS,
                        MemberCategory.DECLARED_FIELDS);
              });
      for (String name :
          new String[] {
            "run.ratchet.store.id.UuidV7EntityListener",
            "run.ratchet.store.converter.InstantAttributeConverter",
            vendor.adapterClassName()
          }) {
        hints
            .reflection()
            .registerType(
                TypeReference.of(name),
                MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
                MemberCategory.INVOKE_DECLARED_METHODS,
                MemberCategory.DECLARED_FIELDS);
      }
      // The target implementation and inherited interfaces supply transaction attributes and
      // default capability methods. Register the exact ordered interface list used by ProxyFactory.
      Class<?> store = ClassUtils.resolveClassName(vendor.storeClassName(), loader);
      registerStore(hints, store);
      hints
          .proxies()
          .registerJdkProxy(
              TypeReference.of(store),
              TypeReference.of("org.springframework.aop.SpringProxy"),
              TypeReference.of("org.springframework.aop.framework.Advised"),
              TypeReference.of("org.springframework.core.DecoratingProxy"));
      hints.proxies().registerJdkProxy(PersistenceProvider.class);
      hints.proxies().registerJdkProxy(PersistenceUnitInfo.class);
      hints.proxies().registerJdkProxy(PersistenceUnitInfo.class, SmartPersistenceUnitInfo.class);
      hints.resources().registerPattern(mapping);
      hints.resources().registerPattern("ddl/**");
      hints.resources().registerPattern(RatchetJpaAotSettings.RESOURCE);
      String applicationResource = "META-INF/ratchet/spring-application-orm.xml";
      if (applicationMapping != null) {
        AotResources.add(generation.getGeneratedFiles(), applicationResource, applicationMapping);
        hints.resources().registerPattern(applicationResource);
      }
      AotResources.add(
          generation.getGeneratedFiles(),
          RatchetJpaAotSettings.RESOURCE,
          "vendor="
              + vendor.name()
              + "\nmapping="
              + mapping
              + "\ndefault-orm="
              + ownership
              + "\n"
              + (applicationMapping == null
                  ? ""
                  : "application-orm=" + applicationResource + "\n"));
    };
  }

  private static void registerStore(RuntimeHints hints, Class<?> type) {
    hints.reflection().registerType(type, MemberCategory.INVOKE_DECLARED_METHODS);
    for (Class<?> parent : type.getInterfaces()) registerStore(hints, parent);
    String implementation = type.getName() + "Impl";
    hints
        .reflection()
        .registerTypeIfPresent(
            type.getClassLoader(),
            implementation,
            MemberCategory.INVOKE_DECLARED_METHODS,
            MemberCategory.INVOKE_PUBLIC_METHODS);
  }

  static String defaultMappingOwnership(ClassLoader loader) {
    return RatchetJpaMappings.defaultMappingOwnership(loader);
  }
}
