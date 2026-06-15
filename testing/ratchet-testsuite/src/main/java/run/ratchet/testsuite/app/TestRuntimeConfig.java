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
package run.ratchet.testsuite.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import run.ratchet.spi.RatchetConfigSource;

/** Reads test runtime settings packaged into the Arquillian deployment. */
public final class TestRuntimeConfig implements RatchetConfigSource {

  private static final String RESOURCE = "ratchet-testsuite.properties";
  private static final Properties PROPERTIES = load();

  public static String dbType() {
    return get("ratchet.test.db.type")
        .orElseGet(() -> System.getProperty("ratchet.test.db.type", "mysql"));
  }

  public static String mongoUri() {
    return get("ratchet.test.mongo.uri")
        .orElseGet(() -> System.getProperty("ratchet.test.mongo.uri"));
  }

  public static String mongoDatabase() {
    return get("ratchet.test.mongo.database")
        .orElseGet(() -> System.getProperty("ratchet.test.mongo.database", "ratchet_test"));
  }

  private static Optional<String> get(String key) {
    if (key == null || key.isBlank()) {
      return Optional.empty();
    }
    String value = PROPERTIES.getProperty(key);
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  private static Properties load() {
    Properties properties = new Properties();
    try (InputStream input =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(RESOURCE)) {
      if (input != null) {
        properties.load(input);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Unable to load " + RESOURCE, e);
    }
    return properties;
  }

  @Override
  public Optional<String> get(String propertyName, String environmentVariable) {
    return get(propertyName).or(() -> get(environmentVariable));
  }
}
