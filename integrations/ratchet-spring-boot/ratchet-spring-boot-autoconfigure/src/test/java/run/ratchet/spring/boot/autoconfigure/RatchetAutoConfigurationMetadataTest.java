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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetConfigCatalog;

class RatchetAutoConfigurationMetadataTest {

  private static final Pattern NAME_PATTERN =
      Pattern.compile("\\\"name\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");

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
    assertEquals(List.of(RatchetAutoConfiguration.class.getName()), Files.readAllLines(imports));
  }

  @Test
  void generatedPropertyMetadataExactlyMatchesTheCanonicalCatalogAndSpringSettings()
      throws Exception {
    Path metadata = classesDirectory().resolve("META-INF/spring-configuration-metadata.json");
    List<String> actualNames = propertyNames(Files.readString(metadata));
    Set<String> distinctNames = new LinkedHashSet<>(actualNames);
    Set<String> expectedNames = new LinkedHashSet<>();
    RatchetConfigCatalog.entries().stream()
        .map(RatchetConfigCatalog.Entry::propertyName)
        .forEach(expectedNames::add);
    expectedNames.add(RatchetProperties.ENABLED_PROPERTY);
    expectedNames.add(RatchetProperties.TRANSACTION_MANAGER_BEAN_NAME_PROPERTY);
    expectedNames.add(RatchetProperties.LIFECYCLE_DEFER_AUTO_START_PROPERTY);
    expectedNames.add(RatchetProperties.LIFECYCLE_DRAIN_TIMEOUT_PROPERTY);

    assertEquals(
        distinctNames.size(), actualNames.size(), "metadata contains duplicate property entries");
    assertEquals(expectedNames, distinctNames);
  }

  private static List<String> propertyNames(String metadata) {
    int propertiesName = metadata.indexOf("\"properties\"");
    int arrayStart = metadata.indexOf('[', propertiesName);
    int arrayEnd = matchingArrayEnd(metadata, arrayStart);
    Matcher matcher = NAME_PATTERN.matcher(metadata.substring(arrayStart, arrayEnd + 1));
    List<String> names = new ArrayList<>();
    while (matcher.find()) {
      names.add(matcher.group(1));
    }
    return names;
  }

  private static int matchingArrayEnd(String json, int arrayStart) {
    int depth = 0;
    boolean quoted = false;
    boolean escaped = false;
    for (int index = arrayStart; index < json.length(); index++) {
      char character = json.charAt(index);
      if (escaped) {
        escaped = false;
      } else if (character == '\\' && quoted) {
        escaped = true;
      } else if (character == '"') {
        quoted = !quoted;
      } else if (!quoted && character == '[') {
        depth++;
      } else if (!quoted && character == ']' && --depth == 0) {
        return index;
      }
    }
    throw new IllegalArgumentException("metadata properties array is not closed");
  }

  private static Path classesDirectory() throws URISyntaxException {
    return Path.of(
        RatchetAutoConfiguration.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }
}
