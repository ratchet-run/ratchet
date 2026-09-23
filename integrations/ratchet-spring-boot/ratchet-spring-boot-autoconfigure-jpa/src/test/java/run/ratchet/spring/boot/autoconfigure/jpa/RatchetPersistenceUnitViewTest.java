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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.persistenceunit.MutablePersistenceUnitInfo;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaMappings;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetPersistenceProvider;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetPersistenceUnitView;

class RatchetPersistenceUnitViewTest {
  @Test
  void metadataViewUsesItsOwnIdentity() {
    PersistenceUnitInfo original = new MutablePersistenceUnitInfo();
    PersistenceUnitInfo view =
        RatchetPersistenceUnitView.create(original, List.of(), List.of(), null, List.of());
    assertIdentity(view, original);
    PersistenceUnitInfo other =
        RatchetPersistenceUnitView.create(original, List.of(), List.of(), null, List.of());
    assertThat(view.equals(other)).isFalse();
    assertThat(other.equals(view)).isFalse();
  }

  @Test
  void providerAdapterUsesItsOwnIdentity() {
    PersistenceProvider original = mock(PersistenceProvider.class);
    LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
    factory.setPersistenceProvider(original);
    RatchetPersistenceProvider.install(factory, (provider, unit) -> unit, true);
    assertIdentity(factory.getPersistenceProvider(), original);
  }

  private static void assertIdentity(Object proxy, Object original) {
    assertThat(proxy.equals(proxy)).isTrue();
    assertThat(proxy.equals(original)).isFalse();
    assertThat(original.equals(proxy)).isFalse();
    assertThat(proxy.equals(null)).isFalse();
    assertThat(proxy.hashCode()).isEqualTo(System.identityHashCode(proxy));
    assertThat(proxy.toString()).isEqualTo(original.toString());
  }

  @Test
  void mutableApplicationMetadataIsNotChangedOrDeduplicated() throws Exception {
    MutablePersistenceUnitInfo original = new MutablePersistenceUnitInfo();
    original.addManagedClassName("example.Host");
    original.addManagedClassName("example.Host");
    original.addMappingFileName("META-INF/host.xml");
    original.addMappingFileName("META-INF/host.xml");
    original.setPersistenceUnitRootUrl(new URL("file:/host/"));
    original.addJarFileUrl(new URL("file:/host.jar"));
    PersistenceUnitInfo first =
        RatchetJpaMappings.transform(
            original, List.of("example.Ratchet", "example.Host"), "META-INF/ratchet.xml");
    PersistenceUnitInfo second =
        RatchetJpaMappings.transform(first, List.of("example.Ratchet"), "META-INF/ratchet.xml");
    assertThat(second.getManagedClassNames())
        .containsExactly("example.Host", "example.Host", "example.Ratchet");
    assertThat(second.getMappingFileNames())
        .containsExactly("META-INF/host.xml", "META-INF/host.xml", "META-INF/ratchet.xml");
    assertThat(original.getManagedClassNames()).containsExactly("example.Host", "example.Host");
    assertThat(original.getMappingFileNames())
        .containsExactly("META-INF/host.xml", "META-INF/host.xml");
    assertThat(second.getPersistenceUnitRootUrl()).isSameAs(original.getPersistenceUnitRootUrl());
    assertThat(second.getJarFileUrls()).containsExactlyElementsOf(original.getJarFileUrls());
    assertThat(second).isInstanceOf(SmartPersistenceUnitInfo.class);
  }

  @Test
  void plainImmutableUnitKeepsItsInterfaceAndOriginalException() {
    PersistenceUnitInfo original = mock(PersistenceUnitInfo.class);
    when(original.getManagedClassNames()).thenReturn(List.of("example.Host"));
    RuntimeException failure = new IllegalStateException("application metadata");
    when(original.getPersistenceUnitName()).thenThrow(failure);
    PersistenceUnitInfo view =
        RatchetPersistenceUnitView.create(
            original,
            original.getManagedClassNames(),
            List.of("META-INF/host.xml"),
            null,
            List.of());
    assertThat(view).isNotInstanceOf(SmartPersistenceUnitInfo.class);
    assertThatThrownBy(view::getPersistenceUnitName).isSameAs(failure);
    assertThat(view.getManagedClassNames()).containsExactly("example.Host");
  }

  @Test
  void sharedProviderTransformsCreationAndSchemaAndPreservesFailures() {
    LocalContainerEntityManagerFactoryBean factory =
        mock(LocalContainerEntityManagerFactoryBean.class);
    PersistenceProvider provider = mock(PersistenceProvider.class);
    when(factory.getPersistenceProvider()).thenReturn(provider);
    PersistenceUnitInfo original = mock(PersistenceUnitInfo.class);
    PersistenceUnitInfo transformed = mock(PersistenceUnitInfo.class);
    RatchetPersistenceProvider.install(
        factory,
        (selectedProvider, unit) -> {
          assertThat(selectedProvider).isSameAs(provider);
          assertThat(unit).isSameAs(original);
          return transformed;
        },
        true);
    ArgumentCaptor<PersistenceProvider> adapter =
        ArgumentCaptor.forClass(PersistenceProvider.class);
    verify(factory).setPersistenceProvider(adapter.capture());
    Map<String, Object> properties = Map.of("application", new Object());
    adapter.getValue().createContainerEntityManagerFactory(original, properties);
    adapter.getValue().generateSchema(original, properties);
    verify(provider).createContainerEntityManagerFactory(same(transformed), same(properties));
    verify(provider).generateSchema(same(transformed), same(properties));
    RuntimeException failure = new IllegalStateException("provider failure");
    when(provider.createContainerEntityManagerFactory(transformed, properties)).thenThrow(failure);
    doThrow(failure).when(provider).generateSchema(transformed, properties);
    assertThatThrownBy(
            () -> adapter.getValue().createContainerEntityManagerFactory(original, properties))
        .isSameAs(failure);
    assertThatThrownBy(() -> adapter.getValue().generateSchema(original, properties))
        .isSameAs(failure);
    adapter.getValue().createEntityManagerFactory("host", properties);
    adapter.getValue().generateSchema("host", properties);
    adapter.getValue().getProviderUtil();
    verify(provider).createEntityManagerFactory("host", properties);
    verify(provider).generateSchema("host", properties);
    verify(provider).getProviderUtil();
  }
}
