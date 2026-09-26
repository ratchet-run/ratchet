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
package run.ratchet.spring.boot.autoconfigure;

import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotContribution;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;
import org.springframework.util.ClassUtils;
import run.ratchet.spring.boot.autoconfigure.internal.AotResources;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaAotSettings;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;

/** Preserves application-only JPA mappings when using a non-SQL Ratchet store in AOT mode. */
public final class RatchetJpaIsolationAotProcessor
    implements BeanFactoryInitializationAotProcessor {
  @Override
  public BeanFactoryInitializationAotContribution processAheadOfTime(
      ConfigurableListableBeanFactory factory) {
    if (!factory.containsBeanDefinition("ratchetJpaIsolationMappings")) return null;
    ClassLoader loader =
        Objects.requireNonNullElseGet(
            factory.getBeanClassLoader(), ClassUtils::getDefaultClassLoader);
    String ownership = RatchetJpaMappings.defaultMappingOwnership(loader);
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
      hints.proxies().registerJdkProxy(PersistenceProvider.class);
      hints.proxies().registerJdkProxy(PersistenceUnitInfo.class);
      hints.proxies().registerJdkProxy(PersistenceUnitInfo.class, SmartPersistenceUnitInfo.class);
      String applicationResource = "META-INF/ratchet/spring-application-orm.xml";
      if (applicationMapping != null) {
        AotResources.add(generation.getGeneratedFiles(), applicationResource, applicationMapping);
        hints.resources().registerPattern(applicationResource);
      }
      AotResources.add(
          generation.getGeneratedFiles(),
          RatchetJpaAotSettings.ISOLATION_RESOURCE,
          "default-orm="
              + ownership
              + "\n"
              + (applicationMapping == null
                  ? ""
                  : "application-orm=" + applicationResource + "\n"));
      hints.resources().registerPattern(RatchetJpaAotSettings.ISOLATION_RESOURCE);
    };
  }
}
