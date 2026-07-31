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

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

class SpringRatchetConfigSourceTest {

  @Test
  void bindsOrdinaryCanonicalProperties() {
    SpringRatchetConfigSource source =
        source(new MapPropertySource("test", Map.of("ratchet.poller.batch-size", "27")));

    assertEquals(
        "27", source.get("ratchet.poller.batch-size", "RATCHET_POLLER_BATCH_SIZE").orElseThrow());
    assertTrue(source.get("", "IGNORED").isEmpty());
  }

  @Test
  void bindsCommaSeparatedClassPolicyAsCanonicalList() {
    SpringRatchetConfigSource source =
        source(
            new MapPropertySource(
                "test", Map.of("ratchet.class-policy.allowed-packages", "com.acme,org.example")));

    assertEquals(
        "com.acme,org.example",
        source
            .get("ratchet.class-policy.allowed-packages", "RATCHET_CLASS_POLICY_ALLOWED_PACKAGES")
            .orElseThrow());
  }

  @Test
  void bindsIndexedClassPolicyEntries() {
    SpringRatchetConfigSource source =
        source(
            new MapPropertySource(
                "test",
                Map.of(
                    "ratchet.class-policy.allowed-result-type-packages[0]", "com.acme.result",
                    "ratchet.class-policy.allowed-result-type-packages[1]", "org.example.result")));

    assertEquals(
        "com.acme.result,org.example.result",
        source
            .get(
                "ratchet.class-policy.allowed-result-type-packages",
                "RATCHET_CLASS_POLICY_ALLOWED_RESULT_TYPE_PACKAGES")
            .orElseThrow());
  }

  @Test
  void bindsRelaxedEnvironmentVariableForms() {
    SpringRatchetConfigSource source =
        source(
            new SystemEnvironmentPropertySource(
                "test", Map.of("RATCHET_CLASS_POLICY_ALLOWED_PACKAGES", "com.acme,org.example")));

    assertEquals(
        "com.acme,org.example",
        source
            .get("ratchet.class-policy.allowed-packages", "RATCHET_CLASS_POLICY_ALLOWED_PACKAGES")
            .orElseThrow());
  }

  private static SpringRatchetConfigSource source(MapPropertySource propertySource) {
    StandardEnvironment environment = new StandardEnvironment();
    environment.getPropertySources().addFirst(propertySource);
    return new SpringRatchetConfigSource(environment);
  }
}
