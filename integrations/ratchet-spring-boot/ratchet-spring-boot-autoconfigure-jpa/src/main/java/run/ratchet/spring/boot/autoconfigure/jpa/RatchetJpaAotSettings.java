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

import java.io.IOException;
import java.util.Properties;
import org.springframework.aot.AotDetector;

/** Build-time decisions carried as resources; contains no database URLs or credentials. */
final class RatchetJpaAotSettings {
  static final String RESOURCE = "META-INF/ratchet/spring-jpa-aot.properties";
  private static final Properties SETTINGS = load();

  private RatchetJpaAotSettings() {}

  static String get(String key) {
    return AotDetector.useGeneratedArtifacts() ? SETTINGS.getProperty(key) : null;
  }

  private static Properties load() {
    Properties properties = new Properties();
    try (var stream = RatchetJpaAotSettings.class.getClassLoader().getResourceAsStream(RESOURCE)) {
      if (stream != null) properties.load(stream);
      return properties;
    } catch (IOException failure) {
      throw new IllegalStateException("Cannot read Ratchet JPA AOT settings", failure);
    }
  }
}
