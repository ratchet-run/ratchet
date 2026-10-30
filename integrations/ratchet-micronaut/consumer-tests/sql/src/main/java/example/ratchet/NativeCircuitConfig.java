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
package example.ratchet;

import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;

@Singleton
@Primary
@Requires(property = "consumer.native-check", value = "true")
public class NativeCircuitConfig implements CircuitBreakerConfigProvider {
  public boolean isEnabled() {
    return true;
  }

  public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
    return new CircuitBreakerConfig(50, 20, 60000, 2, 3);
  }
}
