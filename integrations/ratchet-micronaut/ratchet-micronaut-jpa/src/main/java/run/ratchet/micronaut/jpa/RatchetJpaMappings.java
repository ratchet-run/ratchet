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

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import io.micronaut.core.naming.NameResolver;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import java.util.stream.Stream;
import org.hibernate.boot.MetadataSources;
import run.ratchet.store.schema.RatchetJpaModel;

/** Registers prebuilt Ratchet entities without compile-time or classpath entity scanning. */
@Singleton
@Requires(property = "ratchet.enabled", notEquals = "false")
final class RatchetJpaMappings implements BeanCreatedEventListener<MetadataSources> {
  private final RatchetJpaInfrastructure infrastructure;

  RatchetJpaMappings(RatchetJpaInfrastructure infrastructure) {
    this.infrastructure = infrastructure;
  }

  @Override
  public MetadataSources onCreated(BeanCreatedEvent<MetadataSources> event) {
    // @EachBean chains retain the datasource qualifier on their delegated definition.
    // The registration identifier describes the bean key, not the datasource name.
    String name =
        event.getBeanDefinition() instanceof NameResolver resolver
            ? resolver.resolveName().orElse(null)
            : null;
    var qualifier = event.getBeanDefinition().getDeclaredQualifier();
    if (name == null && qualifier != null) name = Qualifiers.findName(qualifier);
    if (!infrastructure.datasourceName().equals(name)) return event.getBean();
    // Lazy lookup avoids constructing any SessionFactory while its metadata listener is created.
    event.getSource().getBean(RatchetJpaSchemaInitializer.class).initialize();
    MetadataSources sources = event.getBean();
    ClassLoader loader = RatchetJpaMappings.class.getClassLoader();
    Stream.concat(
            RatchetJpaModel.ENTITY_CLASS_NAMES.stream(),
            RatchetJpaModel.CONVERTER_CLASS_NAMES.stream())
        .forEach(
            className -> {
              try {
                sources.addAnnotatedClass(Class.forName(className, false, loader));
              } catch (ClassNotFoundException failure) {
                throw new IllegalStateException("Missing Ratchet JPA class " + className, failure);
              }
            });
    sources.addResource(
        HibernateJpaMappings.mappingFile(SqlStoreVendor.detect(infrastructure.dataSource())));
    // Micronaut uses native MetadataSources.buildMetadata(), not JPA PersistenceUnitInfo.
    // Neither version implicitly loads META-INF/orm.xml; application mapping-resources are
    // appended later by Micronaut. Thus disabled Ratchet needs no filtering provider wrapper.
    return sources;
  }
}
