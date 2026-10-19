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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

import io.micronaut.context.BeanContext;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.core.naming.NameResolver;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.BeanIdentifier;
import io.micronaut.inject.qualifiers.Qualifiers;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Optional;
import javax.sql.DataSource;
import org.hibernate.boot.MetadataSources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.store.schema.RatchetJpaModel;

class RatchetJpaMappingsTest {
  @ParameterizedTest
  @ValueSource(strings = {"default", "jobs"})
  void selectsEachBeanDatasourceAndInitializesSchemaBeforeAddingMappings(String name)
      throws Exception {
    var infrastructure = mock(RatchetJpaInfrastructure.class);
    when(infrastructure.datasourceName()).thenReturn(name);
    var context = mock(BeanContext.class);
    var initializer = mock(RatchetJpaSchemaInitializer.class);
    when(context.getBean(RatchetJpaSchemaInitializer.class)).thenReturn(initializer);
    var dataSource = mock(DataSource.class);
    var connection = mock(Connection.class);
    var metadata = mock(DatabaseMetaData.class);
    when(infrastructure.dataSource()).thenReturn(dataSource);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");
    var listener = new RatchetJpaMappings(infrastructure);
    var sources = mock(MetadataSources.class);

    assertSame(sources, listener.onCreated(event(context, "other", sources, true)));
    verifyNoInteractions(initializer, sources, dataSource);
    assertSame(sources, listener.onCreated(event(context, name, sources, true)));
    var order = inOrder(initializer, sources);
    order.verify(initializer).initialize();
    for (String entity : RatchetJpaModel.ENTITY_CLASS_NAMES)
      order.verify(sources).addAnnotatedClass(Class.forName(entity));
    for (String converter : RatchetJpaModel.CONVERTER_CLASS_NAMES)
      order.verify(sources).addAnnotatedClass(Class.forName(converter));
    order.verify(sources).addResource(HibernateJpaMappings.mappingFile(SqlStoreVendor.POSTGRESQL));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unrelatedDatasourceIsIgnoredForBothDefinitionNamingForms(boolean nameResolver) {
    var infrastructure = mock(RatchetJpaInfrastructure.class);
    when(infrastructure.datasourceName()).thenReturn("jobs");
    var context = mock(BeanContext.class);
    var sources = mock(MetadataSources.class);
    assertSame(
        sources,
        new RatchetJpaMappings(infrastructure)
            .onCreated(event(context, "other", sources, nameResolver)));
    verifyNoInteractions(context, sources);
  }

  @SuppressWarnings("unchecked")
  private static BeanCreatedEvent<MetadataSources> event(
      BeanContext context, String name, MetadataSources sources, boolean nameResolver) {
    BeanDefinition<MetadataSources> definition =
        nameResolver
            ? mock(BeanDefinition.class, withSettings().extraInterfaces(NameResolver.class))
            : mock(BeanDefinition.class);
    if (nameResolver) when(((NameResolver) definition).resolveName()).thenReturn(Optional.of(name));
    when(definition.getDeclaredQualifier()).thenReturn(Qualifiers.byName(name));
    // A real registration identifier describes the bean key and differs from its qualifier.
    var identifier = mock(BeanIdentifier.class);
    when(identifier.getName()).thenReturn("metadata-sources-bean-key");
    return new BeanCreatedEvent<>(
        context, definition, identifier, Argument.of(MetadataSources.class), sources);
  }
}
