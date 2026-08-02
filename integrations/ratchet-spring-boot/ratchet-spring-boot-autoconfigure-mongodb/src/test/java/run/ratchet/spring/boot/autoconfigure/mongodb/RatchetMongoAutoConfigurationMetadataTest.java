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
package run.ratchet.spring.boot.autoconfigure.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.AnnotationUtils;

class RatchetMongoAutoConfigurationMetadataTest {

  @Test
  void buildOutputContainsGeneratedMetadataAndAutoConfigurationImports() throws Exception {
    Path classesDirectory = classesDirectory();
    Path metadata = classesDirectory.resolve("META-INF/spring-configuration-metadata.json");
    Path imports =
        classesDirectory.resolve(
            "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports");

    assertTrue(Files.isRegularFile(metadata), () -> "Missing generated metadata: " + metadata);
    assertTrue(
        Files.isRegularFile(imports), () -> "Missing auto-configuration imports: " + imports);
    assertEquals(
        List.of(RatchetMongoAutoConfiguration.class.getName()), Files.readAllLines(imports));
  }

  @Test
  void wholeAutoConfigurationBacksOffWhenRatchetIsDisabled() {
    ConditionalOnProperty condition =
        AnnotationUtils.findAnnotation(
            RatchetMongoAutoConfiguration.class, ConditionalOnProperty.class);

    assertNotNull(condition);
    assertEquals(List.of("ratchet.enabled"), List.of(condition.name()));
    assertEquals("true", condition.havingValue());
    assertTrue(condition.matchIfMissing());
  }

  @Test
  void generatedMetadataIncludesMongoConnectionProperties() throws Exception {
    Path metadata = classesDirectory().resolve("META-INF/spring-configuration-metadata.json");
    String content = Files.readString(metadata);

    assertTrue(content.contains("\"name\": \"ratchet.mongodb.connection-string\""));
    assertTrue(content.contains("\"name\": \"ratchet.mongodb.database\""));
  }

  private static Path classesDirectory() throws URISyntaxException {
    return Path.of(
        RatchetMongoAutoConfiguration.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toURI());
  }
}
