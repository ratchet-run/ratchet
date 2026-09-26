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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.persistence.spi.PersistenceProvider;
import jakarta.persistence.spi.PersistenceUnitInfo;
import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.GeneratedFiles.Kind;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;
import run.ratchet.spring.boot.autoconfigure.internal.jpa.RatchetJpaAotSettings;

class RatchetJpaIsolationAotProcessorTest {
  @Test
  void backsOffWithoutApplicationJpaIsolation() {
    assertThat(
            new RatchetJpaIsolationAotProcessor()
                .processAheadOfTime(new DefaultListableBeanFactory()))
        .isNull();
  }

  @Test
  void preservesOwnershipAndProxiesWithoutInstantiatingInfrastructure() throws Exception {
    var factory = new DefaultListableBeanFactory();
    factory.registerBeanDefinition(
        "ratchetJpaIsolationMappings", new RootBeanDefinition(Uncreatable.class));
    var hints = new RuntimeHints();
    var files = new InMemoryGeneratedFiles();
    var generation = mock(GenerationContext.class);
    when(generation.getRuntimeHints()).thenReturn(hints);
    when(generation.getGeneratedFiles()).thenReturn(files);
    var processor = new RatchetJpaIsolationAotProcessor();
    processor.processAheadOfTime(factory).applyTo(generation, null);
    processor.processAheadOfTime(factory).applyTo(generation, null);
    assertThat(
            files.getGeneratedFileContent(Kind.RESOURCE, RatchetJpaAotSettings.ISOLATION_RESOURCE))
        .isEqualTo("default-orm=ratchet\n");
    assertThat(RuntimeHintsPredicates.proxies().forInterfaces(PersistenceProvider.class))
        .accepts(hints);
    assertThat(
            RuntimeHintsPredicates.proxies()
                .forInterfaces(PersistenceUnitInfo.class, SmartPersistenceUnitInfo.class))
        .accepts(hints);
    assertThat(
            RuntimeHintsPredicates.resource().forResource(RatchetJpaAotSettings.ISOLATION_RESOURCE))
        .accepts(hints);
    assertThat(factory.containsSingleton("ratchetJpaIsolationMappings")).isFalse();
  }

  static class Uncreatable {
    Uncreatable() {
      throw new AssertionError("AOT must not start persistence infrastructure");
    }
  }
}
