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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/** The application classes registered by Ratchet's build-time AOT processing. */
final class RatchetAotManifest {

  static final String RESOURCE_PATH = "META-INF/ratchet/aot-registered-classes.txt";

  private final Set<String> registeredClasses;

  private RatchetAotManifest(Set<String> registeredClasses) {
    this.registeredClasses = Set.copyOf(registeredClasses);
  }

  /**
   * Loads and parses the AOT manifest once. An absent resource means AOT manifest enforcement is
   * inactive; a present but empty resource is an active empty allowlist.
   */
  static Optional<RatchetAotManifest> load(ClassLoader beanClassLoader) {
    ClassLoader classLoader = beanClassLoader;
    if (classLoader == null) {
      classLoader = Thread.currentThread().getContextClassLoader();
    }
    if (classLoader == null) {
      classLoader = RatchetAotManifest.class.getClassLoader();
    }

    InputStream resource = classLoader.getResourceAsStream(RESOURCE_PATH);
    if (resource == null) {
      return Optional.empty();
    }

    Set<String> registeredClasses = new LinkedHashSet<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String className = line.trim();
        if (!className.isEmpty() && !className.startsWith("#")) {
          registeredClasses.add(className);
        }
      }
    } catch (IOException exception) {
      throw new IllegalStateException(
          "Could not read Ratchet AOT manifest " + RESOURCE_PATH, exception);
    }
    return Optional.of(new RatchetAotManifest(registeredClasses));
  }

  boolean contains(String className) {
    return registeredClasses.contains(className);
  }
}
