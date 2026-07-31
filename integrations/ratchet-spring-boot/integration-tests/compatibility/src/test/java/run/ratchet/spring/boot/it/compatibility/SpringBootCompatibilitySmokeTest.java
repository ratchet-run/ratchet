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
package run.ratchet.spring.boot.it.compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import run.ratchet.ri.runtime.RatchetRuntimeComponentCatalog;
import run.ratchet.spring.boot.autoconfigure.RatchetAutoConfiguration;
import run.ratchet.spring.boot.autoconfigure.jpa.RatchetJpaAutoConfiguration;
import run.ratchet.store.spi.BatchStore;

class SpringBootCompatibilitySmokeTest {

  private static final String CONFIGURATION_METADATA =
      "META-INF/spring-configuration-metadata.json";
  private static final String AUTO_CONFIGURATION_IMPORTS =
      "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports";

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(
                  RatchetAutoConfiguration.class, RatchetJpaAutoConfiguration.class))
          .withPropertyValues("ratchet.allow-empty-class-policy=true");

  @Test
  void sqlStarterKeepsJpaAutoConfigurationGatedWithoutADialectStore() throws Exception {
    contextRunner.run(
        context -> {
          assertTrue(context.isRunning());
          assertNotNull(context.getBean(RatchetAutoConfiguration.class));
          ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
          assertEquals(
              0,
              beanFactory.getBeanNamesForType(RatchetJpaAutoConfiguration.class, true, false)
                  .length);
          assertEquals(0, beanFactory.getBeanNamesForType(BatchStore.class, true, false).length);
          RatchetRuntimeComponentCatalog.components().stream()
              .filter(
                  descriptor ->
                      context.containsBeanDefinition(descriptor.componentType().getName()))
              .forEach(
                  descriptor ->
                      assertFalse(
                          beanFactory.containsSingleton(descriptor.componentType().getName()),
                          () ->
                              "No-store startup eagerly instantiated "
                                  + descriptor.componentType().getName()));
          assertFalse(context.containsBean("dataSource"));
          assertFalse(context.containsBean("entityManagerFactory"));
          assertFalse(context.containsBean("mongo"));
          assertFalse(context.containsBean("mongoTemplate"));
        });

    assertArtifactMetadata(
        RatchetAutoConfiguration.class, RatchetAutoConfiguration.class.getName());
    assertArtifactMetadata(
        RatchetJpaAutoConfiguration.class, RatchetJpaAutoConfiguration.class.getName());
  }

  private static void assertArtifactMetadata(Class<?> anchor, String expectedImport)
      throws IOException, URISyntaxException {
    String metadata = readArtifactResource(anchor, CONFIGURATION_METADATA);
    String imports = readArtifactResource(anchor, AUTO_CONFIGURATION_IMPORTS);

    assertFalse(metadata.isBlank(), () -> "Empty configuration metadata for " + anchor.getName());
    assertEquals(expectedImport, imports.strip());
  }

  private static String readArtifactResource(Class<?> anchor, String resource)
      throws IOException, URISyntaxException {
    Path artifact = Path.of(anchor.getProtectionDomain().getCodeSource().getLocation().toURI());
    if (Files.isDirectory(artifact)) {
      Path resourcePath = artifact.resolve(resource);
      assertTrue(
          Files.isRegularFile(resourcePath), () -> "Missing " + resource + " in " + artifact);
      return Files.readString(resourcePath);
    }

    try (JarFile jar = new JarFile(artifact.toFile())) {
      JarEntry entry = jar.getJarEntry(resource);
      assertNotNull(entry, () -> "Missing " + resource + " in " + artifact);
      return new String(jar.getInputStream(entry).readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
